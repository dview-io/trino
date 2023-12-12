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

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.orc.OrcReaderOptions;
import io.trino.parquet.BloomFilterStore;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.TrinoColumnIndexStore;
import io.trino.plugin.dview.client.DviewClient;
import io.trino.plugin.dview.split.DviewSplit;
import io.trino.plugin.dview.table.DviewTableHandle;
import io.trino.plugin.dview.table.column.DviewColumnHandle;
import io.trino.plugin.dview.utils.FileUtils;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.parquet.ParquetPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
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
import io.trino.spi.type.TypeManager;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.BloomFilterStore.getBloomFilterStore;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.getParquetTypeByName;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.predicate.PredicateUtils.predicateMatches;
import static io.trino.plugin.dview.DviewErrorCode.DVIEW_BAD_DATA;
import static io.trino.plugin.dview.DviewErrorCode.DVIEW_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.dview.DviewErrorCode.DVIEW_CURSOR_ERROR;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public class DviewPageSourceProvider
        implements ConnectorPageSourceProvider
{
    /**
     * If this object is passed as one of the columns for {@code createPageSource},
     * it will be populated as an additional column containing the index of each
     * row read.
     */
    public static final DviewColumnHandle PARQUET_ROW_INDEX_COLUMN = new DviewColumnHandle(
            "$parquet$row_index",
            BIGINT, // no real column index
            -1,
            false,
            null);
    private final DviewClient dviewClient;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final OrcReaderOptions orcReaderOptions;
    private final ParquetReaderOptions parquetReaderOptions;
    private final TypeManager typeManager;
    private TrinoFileSystem fileSystem;
    private static final Logger log = Logger.get(DviewPageSourceProvider.class);

    @Inject
    public DviewPageSourceProvider(DviewClient dviewClient, FileFormatDataSourceStats fileFormatDataSourceStats, TypeManager typeManager)
    {
        this.dviewClient = dviewClient;
        this.fileFormatDataSourceStats = fileFormatDataSourceStats;
        this.orcReaderOptions = new OrcReaderOptions();
        this.parquetReaderOptions = new ParquetReaderOptions();
        this.typeManager = typeManager;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
    {
        DviewSplit dviewSplit = (DviewSplit) split;
        DviewTableHandle dviewTableHandle = (DviewTableHandle) table;
        List<DviewColumnHandle> columnHandles = columns.stream().map(DviewColumnHandle.class::cast).toList();
        TupleDomain<DviewColumnHandle> effectivePredicate = dynamicFilter.getCurrentPredicate().transformKeys(DviewColumnHandle.class::cast);

        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(
                new HdfsEnvironment(
                        new DynamicHdfsConfiguration(
                                new HdfsConfigurationInitializer(
                                        new HdfsConfig(),
                                        Set.of(FileUtils.getConfigurationInitializerForCloudProvider(dviewSplit.getFilePath(), dviewClient.getCloudProviderFor(dviewTableHandle.getEntityId())))),
                                ImmutableSet.of()),
                        new HdfsConfig(),
                        new NoHdfsAuthentication()),
                new TrinoHdfsFileSystemStats());
        fileSystem = fileSystemFactory.create(session);

        Time partitionTime = null;
        Date partitionDate = null;
        if (dviewSplit.getPartitionTime() != null) {
            partitionTime = Time.valueOf(dviewSplit.getPartitionTime());
        }
        if (dviewSplit.getPartitionDate() != null) {
            partitionDate = Date.valueOf(dviewSplit.getPartitionDate());
        }
        boolean useColumnNames = false;
        try {
            return createParquetDataSource(dviewSplit.getFilePath(), columnHandles, useColumnNames, effectivePredicate, partitionDate, partitionTime);
        }
        catch (IOException | RuntimeException e) {
            if (e instanceof TrinoException) {
                throw (TrinoException) e;
            }
            if (e instanceof ParquetCorruptionException) {
                throw new TrinoException(DVIEW_BAD_DATA, e);
            }
            String message = "Error opening Dview split %s : %s".formatted(dviewSplit.getFilePath(), e.getMessage());
            throw new TrinoException(DVIEW_CANNOT_OPEN_SPLIT, message, e);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private ConnectorPageSource createParquetDataSource(String filePath, List<DviewColumnHandle> columnHandles, boolean useColumnNames, TupleDomain<DviewColumnHandle> effectivePredicate, Date partitionDate, Time partitionTime)
            throws InterruptedException, IOException
    {
        int retryCount = 1;
        int sleepIntervalMs = 2000;
        Throwable lastErrorEncountered = null;
        while (retryCount++ <= 3) {
            TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(filePath));
            log.error("reading inputFile: " + inputFile.location());
            try {
                ParquetDataSource dataSource = new TrinoParquetDataSource(inputFile, parquetReaderOptions, fileFormatDataSourceStats);
                DateTimeZone dateTimeZone = UTC;
                int start = 0;
                int domainCompactionThreshold = 100;
                long length = inputFile.length();
                ParquetMetadata parquetMetadata =
                        MetadataReader.readFooter(dataSource, Optional.empty());
                FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
                MessageType fileSchema = fileMetaData.getSchema();
                Optional<MessageType> message =
                        getParquetMessageType(columnHandles, useColumnNames, fileSchema);
                MessageType requestedSchema =
                        message.orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));
                MessageColumnIO messageColumn = getColumnIO(fileSchema, requestedSchema);
                Map<List<String>, ColumnDescriptor> descriptorsByPath =
                        getDescriptors(fileSchema, requestedSchema);
                TupleDomain<ColumnDescriptor> parquetTupleDomain =
                        parquetReaderOptions.isIgnoreStatistics()
                                ? TupleDomain.all()
                                :
                                getParquetTupleDomain(descriptorsByPath, effectivePredicate,
                                        fileSchema,
                                        useColumnNames);
                TupleDomainParquetPredicate parquetPredicate =
                        buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath,
                                dateTimeZone);

                long nextStart = 0;
                ImmutableList.Builder<BlockMetaData> blocks = ImmutableList.builder();
                ImmutableList.Builder<Long> blockStarts = ImmutableList.builder();
                ImmutableList.Builder<Optional<ColumnIndexStore>> columnIndexes =
                        ImmutableList.builder();

                for (BlockMetaData block : parquetMetadata.getBlocks()) {
                    long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                    Optional<ColumnIndexStore> columnIndex =
                            getColumnIndexStore(dataSource, block, descriptorsByPath,
                                    parquetTupleDomain, parquetReaderOptions);
                    Optional<BloomFilterStore> bloomFilterStore =
                            getBloomFilterStore(dataSource, block, parquetTupleDomain,
                                    parquetReaderOptions);

                    if (start <= firstDataPage && firstDataPage < start + length
                            && predicateMatches(
                            parquetPredicate,
                            block,
                            dataSource,
                            descriptorsByPath,
                            parquetTupleDomain,
                            columnIndex,
                            bloomFilterStore,
                            UTC,
                            domainCompactionThreshold)) {
                        blocks.add(block);
                        blockStarts.add(nextStart);
                        columnIndexes.add(columnIndex);
                    }
                    nextStart += block.getRowCount();
                }

                Optional<ReaderColumns> readerProjections =
                        projectBaseColumns(columnHandles, useColumnNames);
                List<DviewColumnHandle> baseColumns = readerProjections.map(projection ->
                                projection.get().stream()
                                        .map(DviewColumnHandle.class::cast)
                                        .toList())
                        .orElse(columnHandles);
                ParquetDataSourceId dataSourceId = dataSource.getId();
                ParquetPageSource.Builder pageSourceBuilder = ParquetPageSource.builder();
                ImmutableList.Builder<Column> parquetColumnFieldsBuilder = ImmutableList.builder();
                int sourceChannel = 0;
                for (DviewColumnHandle column : baseColumns) {
                    if (Objects.equals(column.getColumnName(),
                            PARQUET_ROW_INDEX_COLUMN.getColumnName())) {
                        pageSourceBuilder.addRowIndexColumn();
                    }
                    else if (column.getColumnName().equals("dt") && partitionDate != null) {
                        System.out.println("type : " + column.getColumnType());
                        pageSourceBuilder.addConstantColumn(
                                nativeValueToBlock(column.getColumnType(),
                                        partitionDate.getTime() / 86400000));
                    }
                    else if (column.getColumnName().equals("hour") && partitionTime != null) {
                        pageSourceBuilder.addConstantColumn(
                                nativeValueToBlock(column.getColumnType(),
                                        partitionTime.getTime()));
                    }
                    else {
                        Optional<org.apache.parquet.schema.Type> parquetType =
                                getBaseColumnParquetType(column, fileSchema, useColumnNames);
                        if (parquetType.isEmpty()) {
                            pageSourceBuilder.addNullColumn(column.getColumnType());
                            continue;
                        }
                        String columnName = useColumnNames ? column.getColumnName() :
                                fileSchema.getFields().get(column.getOrdinalPosition()).getName();
                        Optional<Field> field = constructField(column.getColumnType(),
                                lookupColumnByName(messageColumn, columnName));
                        if (field.isEmpty()) {
                            pageSourceBuilder.addNullColumn(column.getColumnType());
                            continue;
                        }
                        parquetColumnFieldsBuilder.add(new Column(columnName, field.get()));
                        pageSourceBuilder.addSourceColumn(sourceChannel);
                        sourceChannel++;
                    }
                }
                ParquetPageSourceFactory.ParquetReaderProvider parquetReaderProvider =
                        fields -> new ParquetReader(
                                Optional.ofNullable(fileMetaData.getCreatedBy()),
                                fields,
                                blocks.build(),
                                blockStarts.build(),
                                dataSource,
                                dateTimeZone,
                                newSimpleAggregatedMemoryContext(),
                                parquetReaderOptions,
                                exception -> handleException(dataSourceId, exception),
                                Optional.of(parquetPredicate),
                                columnIndexes.build(),
                                Optional.empty());
                return pageSourceBuilder.build(
                        parquetReaderProvider.createParquetReader(
                                parquetColumnFieldsBuilder.build()));
            }
            catch (FileNotFoundException fileNotFoundException) {
                // File got deleted during overwrite
                int sleepMs = retryCount * sleepIntervalMs;
                log.error("File not found, File maybe deleted for overwrite, retrying in {0} ms", sleepMs);
                Thread.sleep(sleepMs);
                lastErrorEncountered = fileNotFoundException;
            }
            catch (IOException ioException) {
                // ParquetCorruptionException
                // File maybe being read while getting overwritten
                int sleepMs = retryCount * sleepIntervalMs;
                log.error("File maybe getting overwritten, retrying in {0} ms", sleepMs);
                Thread.sleep(sleepMs);
                lastErrorEncountered = ioException;
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        throw new IOException(lastErrorEncountered);
    }

    private static TupleDomain<ColumnDescriptor> getParquetTupleDomain(Map<List<String>, ColumnDescriptor> descriptorsByPath, TupleDomain<DviewColumnHandle> effectivePredicate, MessageType fileSchema, boolean useColumnNames)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        for (Map.Entry<DviewColumnHandle, Domain> entry : effectivePredicate.getDomains().get().entrySet()) {
            DviewColumnHandle columnHandle = entry.getKey();

            ColumnDescriptor descriptor = null;

            Optional<org.apache.parquet.schema.Type> baseColumnType = getBaseColumnParquetType(columnHandle, fileSchema, useColumnNames);
            // Parquet file has fewer column than partition
            if (baseColumnType.isEmpty()) {
                continue;
            }

            if (baseColumnType.get().isPrimitive()) {
                descriptor = descriptorsByPath.get(ImmutableList.of(baseColumnType.get().getName()));
            }

            if (descriptor != null) {
                predicate.put(descriptor, entry.getValue());
            }
        }
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

    public static Optional<ColumnIndexStore> getColumnIndexStore(
            ParquetDataSource dataSource,
            BlockMetaData blockMetadata,
            Map<List<String>, ColumnDescriptor> descriptorsByPath,
            TupleDomain<ColumnDescriptor> parquetTupleDomain,
            ParquetReaderOptions options)
    {
        if (!options.isUseColumnIndex() || parquetTupleDomain.isAll() || parquetTupleDomain.isNone()) {
            return Optional.empty();
        }

        boolean hasColumnIndex = false;
        for (ColumnChunkMetaData column : blockMetadata.getColumns()) {
            if (column.getColumnIndexReference() != null && column.getOffsetIndexReference() != null) {
                hasColumnIndex = true;
                break;
            }
        }

        if (!hasColumnIndex) {
            return Optional.empty();
        }

        Set<ColumnPath> columnsReadPaths = new HashSet<>(descriptorsByPath.size());
        for (List<String> path : descriptorsByPath.keySet()) {
            columnsReadPaths.add(ColumnPath.get(path.toArray(new String[0])));
        }

        Map<ColumnDescriptor, Domain> parquetDomains = parquetTupleDomain.getDomains()
                .orElseThrow(() -> new IllegalStateException("Predicate other than none should have domains"));
        Set<ColumnPath> columnsFilteredPaths = parquetDomains.keySet().stream()
                .map(column -> ColumnPath.get(column.getPath()))
                .collect(toImmutableSet());

        return Optional.of(new TrinoColumnIndexStore(dataSource, blockMetadata, columnsReadPaths, columnsFilteredPaths));
    }
//    public static Optional<Field> constructField(io.trino.spi.type.Type trinoType, ColumnIO columnIO)
//    {
//        if (columnIO == null) {
//            return Optional.empty();
//        }
//        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
////        int repetitionLevel = columnIO.getRepetitionLevel();
////        int definitionLevel = columnIO.getDefinitionLevel();
//        io.trino.spi.type.Type type = column.getColumnType();
////        if (type instanceof RowType rowType) {
////            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
////            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
////            List<RowType.Field> fields = rowType.getFields();
////            boolean structHasParameters = false;
////            for (int i = 0; i < fields.size(); i++) {
////                RowType.Field rowField = fields.get(i);
////                ColumnIdentity fieldIdentity = subColumns.get(i);
////                Optional<Field> field = constructField(new FieldContext(rowField.getType(), fieldIdentity), lookupColumnById(groupColumnIO, fieldIdentity.getId()));
////                structHasParameters |= field.isPresent();
////                fieldsBuilder.add(field);
////            }
////            if (structHasParameters) {
////                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, fieldsBuilder.build()));
////            }
////            return Optional.empty();
////        }
////        if (type instanceof MapType mapType) {
////            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
////            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
////            if (keyValueColumnIO.getChildrenCount() != 2) {
////                return Optional.empty();
////            }
////            List<ColumnIdentity> subColumns = context.getColumnIdentity().getChildren();
////            checkArgument(subColumns.size() == 2, "Not a map: %s", context);
////            ColumnIdentity keyIdentity = subColumns.get(0);
////            ColumnIdentity valueIdentity = subColumns.get(1);
////            // TODO validate column ID
////            Optional<Field> keyField = constructField(new FieldContext(mapType.getKeyType(), keyIdentity), keyValueColumnIO.getChild(0));
////            // TODO validate column ID
////            Optional<Field> valueField = constructField(new FieldContext(mapType.getValueType(), valueIdentity), keyValueColumnIO.getChild(1));
////            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
////        }
////        if (type instanceof ArrayType arrayType) {
////            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
////            if (groupColumnIO.getChildrenCount() != 1) {
////                return Optional.empty();
////            }
////            List<ColumnIdentity> subColumns = context.getColumnIdentity().getChildren();
////            checkArgument(subColumns.size() == 1, "Not an array: %s", context);
////            ColumnIdentity elementIdentity = getOnlyElement(subColumns);
////            // TODO validate column ID
////            Optional<Field> field = constructField(new FieldContext(arrayType.getElementType(), elementIdentity), getArrayElementColumn(groupColumnIO.getChild(0)));
////            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(field)));
////        }
//        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
//        return Optional.of(new PrimitiveField(type, required, primitiveColumnIO.getColumnDescriptor(), primitiveColumnIO.getId()));
//    }

    private static TrinoException handleException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        if (exception instanceof ParquetCorruptionException) {
            return new TrinoException(DVIEW_BAD_DATA, exception);
        }
        System.out.println(Arrays.toString(exception.getStackTrace()));
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

    public static Optional<MessageType> getParquetMessageType(List<DviewColumnHandle> columns, boolean useColumnNames, MessageType fileSchema)
    {
//        Optional<MessageType> message = projectSufficientColumns(columns)
//                .map(projection -> projection.get().stream()
//                        .map(HiveColumnHandle.class::cast)
//                        .toList())
//                .orElse(columns).stream()
//                .filter(column -> column.getColumnType() == REGULAR)
//                .map(column -> getColumnType(column, fileSchema, useColumnNames))
//                .filter(Optional::isPresent)
//                .map(Optional::get)
//                .map(type -> new MessageType(fileSchema.getName(), type))
//                .reduce(MessageType::union);
//        return message;
        return Optional.of(fileSchema);
    }

    /**
     * Creates a set of sufficient columns for the input projected columns and prepares a mapping between the two. For example,
     * if input {@param columns} include columns "a.b" and "a.b.c", then they will be projected from a single column "a.b".
     */
    public static Optional<ReaderColumns> projectSufficientColumns(List<HiveColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");

        if (columns.stream().allMatch(HiveColumnHandle::isBaseColumn)) {
            return Optional.empty();
        }

        ImmutableBiMap.Builder<DereferenceChain, HiveColumnHandle> dereferenceChainsBuilder = ImmutableBiMap.builder();

        for (HiveColumnHandle column : columns) {
            List<Integer> indices = column.getHiveColumnProjectionInfo()
                    .map(HiveColumnProjectionInfo::getDereferenceIndices)
                    .orElse(ImmutableList.of());

            DereferenceChain dereferenceChain = new DereferenceChain(column.getBaseColumnName(), indices);
            dereferenceChainsBuilder.put(dereferenceChain, column);
        }

        BiMap<DereferenceChain, HiveColumnHandle> dereferenceChains = dereferenceChainsBuilder.build();

        List<ColumnHandle> sufficientColumns = new ArrayList<>();
        ImmutableList.Builder<Integer> outputColumnMapping = ImmutableList.builder();

        Map<DereferenceChain, Integer> pickedColumns = new HashMap<>();

        // Pick a covering column for every column
        for (HiveColumnHandle columnHandle : columns) {
            DereferenceChain column = dereferenceChains.inverse().get(columnHandle);
            List<DereferenceChain> orderedPrefixes = column.getOrderedPrefixes();
            DereferenceChain chosenColumn = null;

            // Shortest existing prefix is chosen as the input.
            for (DereferenceChain prefix : orderedPrefixes) {
                if (dereferenceChains.containsKey(prefix)) {
                    chosenColumn = prefix;
                    break;
                }
            }

            checkState(chosenColumn != null, "chosenColumn is null");
            int inputBlockIndex;

            if (pickedColumns.containsKey(chosenColumn)) {
                // Use already picked column
                inputBlockIndex = pickedColumns.get(chosenColumn);
            }
            else {
                // Add a new column for the reader
                sufficientColumns.add(dereferenceChains.get(chosenColumn));
                pickedColumns.put(chosenColumn, sufficientColumns.size() - 1);
                inputBlockIndex = sufficientColumns.size() - 1;
            }

            outputColumnMapping.add(inputBlockIndex);
        }

        return Optional.of(new ReaderColumns(sufficientColumns, outputColumnMapping.build()));
    }

    public static Optional<org.apache.parquet.schema.Type> getColumnType(DviewColumnHandle column, MessageType messageType, boolean useParquetColumnNames)
    {
        Optional<org.apache.parquet.schema.Type> baseColumnType = getBaseColumnParquetType(column, messageType, useParquetColumnNames);
        if (baseColumnType.isEmpty()) {
            return baseColumnType;
        }
        GroupType baseType = baseColumnType.get().asGroupType();
//        Optional<List<org.apache.parquet.schema.Type>> subFieldTypesOptional = dereferenceSubFieldTypes(baseType, column.getHiveColumnProjectionInfo().get());
//
//        // if there is a mismatch between parquet schema and the hive schema and the column cannot be dereferenced
//        if (subFieldTypesOptional.isEmpty()) {
//            return Optional.empty();
//        }
//
//        List<org.apache.parquet.schema.Type> subfieldTypes = subFieldTypesOptional.get();
//        org.apache.parquet.schema.Type type = subfieldTypes.get(subfieldTypes.size() - 1);
//        for (int i = subfieldTypes.size() - 2; i >= 0; --i) {
//            GroupType groupType = subfieldTypes.get(i).asGroupType();
//            type = new GroupType(groupType.getRepetition(), groupType.getName(), ImmutableList.of(type));
//        }
//        return Optional.of(new GroupType(baseType.getRepetition(), baseType.getName()));
        return Optional.of(baseType);
    }

    private static Optional<org.apache.parquet.schema.Type> getBaseColumnParquetType(DviewColumnHandle column, MessageType messageType, boolean useParquetColumnNames)
    {
        if (useParquetColumnNames) {
            return Optional.ofNullable(getParquetTypeByName(column.getColumnName(), messageType));
        }
        if (column.getOrdinalPosition() < messageType.getFieldCount()) {
            return Optional.of(messageType.getType(column.getOrdinalPosition()));
        }
        return Optional.empty();
    }

    /**
     * Dereferencing base parquet type based on projection info's dereference names.
     * For example, when dereferencing baseType(level1Field0, level1Field1, Level1Field2(Level2Field0, Level2Field1))
     * with a projection info's dereferenceNames list as (basetype, Level1Field2, Level2Field1).
     * It would return a list of parquet types in the order of (level1Field2, Level2Field1)
     *
     * @return child fields on each level of dereferencing. Return Optional.empty when failed to do the lookup.
     */
    private static Optional<List<org.apache.parquet.schema.Type>> dereferenceSubFieldTypes(GroupType baseType, HiveColumnProjectionInfo projectionInfo)
    {
        checkArgument(baseType != null, "base type cannot be null when dereferencing");
        checkArgument(projectionInfo != null, "hive column projection info cannot be null when doing dereferencing");

        ImmutableList.Builder<org.apache.parquet.schema.Type> typeBuilder = ImmutableList.builder();
        org.apache.parquet.schema.Type parentType = baseType;

        for (String name : projectionInfo.getDereferenceNames()) {
            org.apache.parquet.schema.Type childType = getParquetTypeByName(name, parentType.asGroupType());
            if (childType == null) {
                return Optional.empty();
            }
            typeBuilder.add(childType);
            parentType = childType;
        }

        return Optional.of(typeBuilder.build());
    }

    /**
     * Creates a mapping between the input {@param columns} and base columns based on baseHiveColumnIndex or baseColumnName if required.
     */
    public static Optional<ReaderColumns> projectBaseColumns(List<DviewColumnHandle> columns, boolean useColumnNames)
    {
        requireNonNull(columns, "columns is null");
        return Optional.empty();

        // No projection is required if all columns are base columns
//        if (columns.stream().allMatch(HiveColumnHandle::isBaseColumn)) {
//            return Optional.empty();
//        }
//
//        ImmutableList.Builder<ColumnHandle> projectedColumns = ImmutableList.builder();
//        ImmutableList.Builder<Integer> outputColumnMapping = ImmutableList.builder();
//        Map<Object, Integer> mappedHiveBaseColumnKeys = new HashMap<>();
//        int projectedColumnCount = 0;
//
//        for (HiveColumnHandle column : columns) {
//            Object baseColumnKey = useColumnNames ? column.getBaseColumnName() : column.getBaseHiveColumnIndex();
//            Integer mapped = mappedHiveBaseColumnKeys.get(baseColumnKey);
//
//            if (mapped == null) {
//                projectedColumns.add(column.getBaseColumn());
//                mappedHiveBaseColumnKeys.put(baseColumnKey, projectedColumnCount);
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

    private static class DereferenceChain
    {
        private final String name;
        private final List<Integer> indices;

        public DereferenceChain(String name, List<Integer> indices)
        {
            this.name = requireNonNull(name, "name is null");
            this.indices = ImmutableList.copyOf(requireNonNull(indices, "indices is null"));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DereferenceChain that = (DereferenceChain) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(indices, that.indices);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, indices);
        }

        /**
         * Get Prefixes of this Dereference chain in increasing order of lengths
         */
        public List<DereferenceChain> getOrderedPrefixes()
        {
            ImmutableList.Builder<DereferenceChain> prefixes = ImmutableList.builder();

            for (int prefixLen = 0; prefixLen <= indices.size(); prefixLen++) {
                prefixes.add(new DereferenceChain(name, indices.subList(0, prefixLen)));
            }

            return prefixes.build();
        }
    }
}
