/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.dview.page;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.OrcReaderOptions;
import io.trino.parquet.BloomFilterStore;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.plugin.dview.split.DviewSplit;
import io.trino.plugin.dview.table.DviewTableHandle;
import io.trino.plugin.dview.table.column.DviewColumnHandle;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeManager;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.BloomFilterStore.getBloomFilterStore;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.predicate.PredicateUtils.predicateMatches;
import static io.trino.plugin.dview.DviewErrorCode.DVIEW_BAD_DATA;
import static io.trino.plugin.dview.DviewErrorCode.DVIEW_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.dview.DviewErrorCode.DVIEW_CURSOR_ERROR;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.joda.time.DateTimeZone.UTC;

public class DviewPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final OrcReaderOptions orcReaderOptions;
    private final ParquetReaderOptions parquetReaderOptions;
    private final TypeManager typeManager;

    @Inject
    public DviewPageSourceProvider(TrinoFileSystemFactory fileSystemFactory, FileFormatDataSourceStats fileFormatDataSourceStats, OrcReaderOptions orcReaderOptions, ParquetReaderOptions parquetReaderOptions, TypeManager typeManager)
    {
        this.fileSystemFactory = fileSystemFactory;
        this.fileFormatDataSourceStats = fileFormatDataSourceStats;
        this.orcReaderOptions = orcReaderOptions;
        this.parquetReaderOptions = parquetReaderOptions;
        this.typeManager = typeManager;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
    {
        DviewSplit dviewSplit = (DviewSplit) split;
        DviewTableHandle dviewTableHandle = (DviewTableHandle) table;

        List<DviewColumnHandle> columnHandles = columns.stream().map(DviewColumnHandle.class::cast).toList();
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(dviewSplit.getFilePath()));
        int start = 0;
        TupleDomain<DviewColumnHandle> effectivePredicate = dynamicFilter.getCurrentPredicate().transformKeys(DviewColumnHandle.class::cast);
        ParquetDataSource dataSource = null;
        Time partitionTime = null;
        Date partitionDate = null;
        if (dviewSplit.getPartitionTime() != null) {
            partitionTime = Time.valueOf(dviewSplit.getPartitionTime());
        }
        if (dviewSplit.getPartitionDate() != null) {
            partitionDate = Date.valueOf(dviewSplit.getPartitionDate());
        }
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        try {
            long length = inputFile.length();
            dataSource = new TrinoParquetDataSource(inputFile, parquetReaderOptions, fileFormatDataSourceStats);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();
            Map<Integer, Type> parquetIdToField = fileSchema.getFields().stream()
                    .filter(field -> field.getId() != null)
                    .collect(toImmutableMap(field -> field.getId().intValue(), Function.identity()));
            List<DviewColumnHandle> readColumns = columnHandles;
            List<org.apache.parquet.schema.Type> parquetFields = readColumns.stream()
                    .map(column -> parquetIdToField.get(column.getOrdinalPosition()))
                    .toList();
            MessageType requestedSchema = new MessageType(fileSchema.getName(), parquetFields.stream().filter(Objects::nonNull).collect(toImmutableList()));
            Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate);
            TupleDomainParquetPredicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath, DateTimeZone.UTC);
            long nextStart = 0;
            Optional<Long> startRowPosition = Optional.empty();
            Optional<Long> endRowPosition = Optional.empty();
            Optional<ReaderColumns> columnProjections = projectColumns(readColumns);
            ImmutableList.Builder<Long> blockStarts = ImmutableList.builder();
            List<BlockMetaData> blocks = new ArrayList<>();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                Optional<BloomFilterStore> bloomFilterStore = getBloomFilterStore(dataSource, block, parquetTupleDomain, parquetReaderOptions);
                if (start <= firstDataPage && firstDataPage < start + length &&
                        predicateMatches(parquetPredicate, block, dataSource, descriptorsByPath, parquetTupleDomain, Optional.empty(), bloomFilterStore, UTC, 1000)) {
                    blocks.add(block);
                    blockStarts.add(nextStart);
                    if (startRowPosition.isEmpty()) {
                        startRowPosition = Optional.of(nextStart);
                    }
                    endRowPosition = Optional.of(nextStart + block.getRowCount());
                }
                nextStart += block.getRowCount();
            }
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            ParquetPageSource.Builder pageSourceBuilder = ParquetPageSource.builder();
            int parquetSourceChannel = 0;
            ImmutableList.Builder<Field> parquetColumnFieldsBuilder = ImmutableList.builder();
            for (int columnIndex = 0; columnIndex < readColumns.size(); columnIndex++) {
                DviewColumnHandle column = readColumns.get(columnIndex);
                if (column.getColumnName().equals("dt")) {
                    pageSourceBuilder.addConstantColumn(nativeValueToBlock(column.getColumnType(), partitionDate));
                }
                else if (column.getColumnName().equals("hour")) {
                    pageSourceBuilder.addConstantColumn(nativeValueToBlock(column.getColumnType(), partitionTime));
                }
                else {
                    org.apache.parquet.schema.Type parquetField = parquetFields.get(columnIndex);
                    io.trino.spi.type.Type trinoType = column.getColumnType();
                    if (parquetField == null) {
                        pageSourceBuilder.addNullColumn(trinoType);
                        continue;
                    }
                    // The top level columns are already mapped by name/id appropriately.
                    ColumnIO columnIO = messageColumnIO.getChild(parquetField.getName());
                    Optional<Field> field = constructField(trinoType, column, columnIO);
                    if (field.isEmpty()) {
                        pageSourceBuilder.addNullColumn(trinoType);
                        continue;
                    }
                    parquetColumnFieldsBuilder.add(field.get());
                    pageSourceBuilder.addSourceColumn(parquetSourceChannel);
                    parquetSourceChannel++;
                }
            }
            ParquetDataSourceId dataSourceId = dataSource.getId();
            ParquetReader parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    parquetColumnFieldsBuilder.build(),
                    blocks,
                    blockStarts.build(),
                    dataSource,
                    UTC,
                    memoryContext,
                    parquetReaderOptions,
                    exception -> handleException(dataSourceId, exception));
            return pageSourceBuilder.build(parquetReader);
        }
        catch (IOException | RuntimeException e) {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException ex) {
                if (!e.equals(ex)) {
                    e.addSuppressed(ex);
                }
            }
            if (e instanceof TrinoException) {
                throw (TrinoException) e;
            }
            if (e instanceof ParquetCorruptionException) {
                throw new TrinoException(DVIEW_BAD_DATA, e);
            }
            String message = "Error opening Dview split %s : %s".formatted(inputFile.location(), e.getMessage());
            throw new TrinoException(DVIEW_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static TupleDomain<ColumnDescriptor> getParquetTupleDomain(Map<List<String>, ColumnDescriptor> descriptorsByPath, TupleDomain<DviewColumnHandle> effectivePredicate)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        effectivePredicate.getDomains().orElseThrow().forEach((columnHandle, domain) -> {
            String baseType = columnHandle.getColumnType().getBaseName();
            // skip looking up predicates for complex types as Parquet only stores stats for primitives
            if ((!baseType.equals(StandardTypes.MAP) && !baseType.equals(StandardTypes.ARRAY) && !baseType.equals(StandardTypes.ROW))) {
                ColumnDescriptor descriptor = descriptorsByPath.get(ImmutableList.of(columnHandle.getColumnName()));
                if (descriptor != null) {
                    predicate.put(descriptor, domain);
                }
            }
        });
        return TupleDomain.withColumnDomains(predicate.buildOrThrow());
    }

    public static TupleDomainParquetPredicate buildPredicate(
            MessageType requestedSchema,
            TupleDomain<ColumnDescriptor> parquetTupleDomain,
            Map<List<String>, ColumnDescriptor> descriptorsByPath,
            DateTimeZone timeZone)
    {
        ImmutableList.Builder<ColumnDescriptor> columnReferences = ImmutableList.builder();
        for (String[] paths : requestedSchema.getPaths()) {
            ColumnDescriptor descriptor = descriptorsByPath.get(Arrays.asList(paths));
            if (descriptor != null) {
                columnReferences.add(descriptor);
            }
        }
        return new TupleDomainParquetPredicate(parquetTupleDomain, columnReferences.build(), timeZone);
    }

    public static Optional<Field> constructField(io.trino.spi.type.Type trinoType, DviewColumnHandle column, ColumnIO columnIO)
    {
        if (columnIO == null) {
            return Optional.empty();
        }
        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
//        int repetitionLevel = columnIO.getRepetitionLevel();
//        int definitionLevel = columnIO.getDefinitionLevel();
        io.trino.spi.type.Type type = column.getColumnType();
//        if (type instanceof RowType rowType) {
//            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
//            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
//            List<RowType.Field> fields = rowType.getFields();
//            boolean structHasParameters = false;
//            for (int i = 0; i < fields.size(); i++) {
//                RowType.Field rowField = fields.get(i);
//                ColumnIdentity fieldIdentity = subColumns.get(i);
//                Optional<Field> field = constructField(new FieldContext(rowField.getType(), fieldIdentity), lookupColumnById(groupColumnIO, fieldIdentity.getId()));
//                structHasParameters |= field.isPresent();
//                fieldsBuilder.add(field);
//            }
//            if (structHasParameters) {
//                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, fieldsBuilder.build()));
//            }
//            return Optional.empty();
//        }
//        if (type instanceof MapType mapType) {
//            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
//            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
//            if (keyValueColumnIO.getChildrenCount() != 2) {
//                return Optional.empty();
//            }
//            List<ColumnIdentity> subColumns = context.getColumnIdentity().getChildren();
//            checkArgument(subColumns.size() == 2, "Not a map: %s", context);
//            ColumnIdentity keyIdentity = subColumns.get(0);
//            ColumnIdentity valueIdentity = subColumns.get(1);
//            // TODO validate column ID
//            Optional<Field> keyField = constructField(new FieldContext(mapType.getKeyType(), keyIdentity), keyValueColumnIO.getChild(0));
//            // TODO validate column ID
//            Optional<Field> valueField = constructField(new FieldContext(mapType.getValueType(), valueIdentity), keyValueColumnIO.getChild(1));
//            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
//        }
//        if (type instanceof ArrayType arrayType) {
//            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
//            if (groupColumnIO.getChildrenCount() != 1) {
//                return Optional.empty();
//            }
//            List<ColumnIdentity> subColumns = context.getColumnIdentity().getChildren();
//            checkArgument(subColumns.size() == 1, "Not an array: %s", context);
//            ColumnIdentity elementIdentity = getOnlyElement(subColumns);
//            // TODO validate column ID
//            Optional<Field> field = constructField(new FieldContext(arrayType.getElementType(), elementIdentity), getArrayElementColumn(groupColumnIO.getChild(0)));
//            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(field)));
//        }
        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        return Optional.of(new PrimitiveField(type, required, primitiveColumnIO.getColumnDescriptor(), primitiveColumnIO.getId()));
    }

    private static TrinoException handleException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        if (exception instanceof ParquetCorruptionException) {
            return new TrinoException(DVIEW_BAD_DATA, exception);
        }
        return new TrinoException(DVIEW_CURSOR_ERROR, format("Failed to read Parquet file: %s", dataSourceId), exception);
    }

    /**
     * Creates a mapping between the input {@code columns} and base columns if required.
     */
    public static Optional<ReaderColumns> projectColumns(List<DviewColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");
        return Optional.empty();
        // No projection is required if all columns are base columns
//        if (columns.stream().allMatch(IcebergColumnHandle::isBaseColumn)) {
//            return Optional.empty();
//        }
//
//        ImmutableList.Builder<ColumnHandle> projectedColumns = ImmutableList.builder();
//        ImmutableList.Builder<Integer> outputColumnMapping = ImmutableList.builder();
//        Map<Integer, Integer> mappedFieldIds = new HashMap<>();
//        int projectedColumnCount = 0;
//
//        for (IcebergColumnHandle column : columns) {
//            int baseColumnId = column.getBaseColumnIdentity().getId();
//            Integer mapped = mappedFieldIds.get(baseColumnId);
//
//            if (mapped == null) {
//                projectedColumns.add(column.getBaseColumn());
//                mappedFieldIds.put(baseColumnId, projectedColumnCount);
//                outputColumnMapping.add(projectedColumnCount);
//                projectedColumnCount++;
//            }
//            else {
//                outputColumnMapping.add(mapped);
//            }
//        }
//
//        return Optional.of(new ReaderColumns(projectedColumns.build(), outputColumnMapping.build()));
    }
}
