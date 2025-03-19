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
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.metadata.PrunedBlockMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.parquet.reader.TrinoColumnIndexStore;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.dview.DviewConfig;
import io.trino.plugin.dview.client.DviewClient;
import io.trino.plugin.dview.split.DviewSplit;
import io.trino.plugin.dview.table.DviewTableHandle;
import io.trino.plugin.dview.table.column.DviewColumnHandle;
import io.trino.plugin.dview.utils.FileUtils;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
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
import io.trino.spi.type.TypeManager;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnPath;
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
import static io.trino.parquet.metadata.PrunedBlockMetadata.createPrunedColumnsMetadata;
import static io.trino.parquet.predicate.PredicateUtils.predicateMatches;
import static io.trino.plugin.dview.DviewErrorCode.DVIEW_BAD_DATA;
import static io.trino.plugin.dview.DviewErrorCode.DVIEW_CANNOT_OPEN_SPLIT;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public class DviewPageSourceProvider
        implements ConnectorPageSourceProvider
{
    public static final DviewColumnHandle PARQUET_ROW_INDEX_COLUMN = new DviewColumnHandle(
            "$parquet$row_index",
            BIGINT,
            -1,
            false,
            null);
    private final DviewClient dviewClient;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final OrcReaderOptions orcReaderOptions;
    private final ParquetReaderOptions parquetReaderOptions;
    private final TypeManager typeManager;
    private TrinoFileSystem fileSystem;
    private final DviewConfig dviewConfig;
    private static final Logger log = Logger.get(DviewPageSourceProvider.class);

    @Inject
    public DviewPageSourceProvider(DviewClient dviewClient, FileFormatDataSourceStats fileFormatDataSourceStats, TypeManager typeManager, DviewConfig dviewConfig)
    {
        this.dviewClient = dviewClient;
        this.fileFormatDataSourceStats = fileFormatDataSourceStats;
        this.dviewConfig = dviewConfig;
        this.orcReaderOptions = new OrcReaderOptions();
        this.parquetReaderOptions = new ParquetReaderOptions();
        this.typeManager = typeManager;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
    {
        log.info("Entering into DviewPageSourceProvider::createPageSource");
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

    private ConnectorPageSource createParquetDataSource(
            String filePath,
            List<DviewColumnHandle> columnHandles,
            boolean useColumnNames,
            TupleDomain<DviewColumnHandle> effectivePredicate,
            Date partitionDate,
            Time partitionTime)
            throws InterruptedException, IOException
    {
        log.info("Entering into DviewPageSourceProvider::createParquetDataSource");
        int retryCount = 1;
        int sleepIntervalMs = 2000;
        Throwable lastErrorEncountered = null;
        while (retryCount++ <= 3) {
            TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(filePath));
            log.info("reading inputFile: " + inputFile.location());
            try {
                ParquetDataSource dataSource = new TrinoParquetDataSource(inputFile, parquetReaderOptions, fileFormatDataSourceStats);
                DateTimeZone dateTimeZone = UTC;
                int start = 0;
                int domainCompactionThreshold = 100;
                long length = inputFile.length();
                ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
                FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
                MessageType fileSchema = fileMetaData.getSchema();
                Optional<MessageType> message = getParquetMessageType(columnHandles, useColumnNames, fileSchema);
                MessageType requestedSchema = message.orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));
                MessageColumnIO messageColumn = getColumnIO(fileSchema, requestedSchema);
                Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
                TupleDomain<ColumnDescriptor> parquetTupleDomain = parquetReaderOptions.isIgnoreStatistics()
                        ? TupleDomain.all()
                        : getParquetTupleDomain(descriptorsByPath, effectivePredicate, fileSchema, useColumnNames);
                TupleDomainParquetPredicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath, dateTimeZone);

                long nextStart = 0;
                ImmutableList.Builder<RowGroupInfo> rowGroups = ImmutableList.builder();
                for (BlockMetadata block : parquetMetadata.getBlocks()) {
                    long firstDataPage = block.columns().getFirst().getFirstDataPageOffset();
                    Optional<ColumnIndexStore> columnIndex = getColumnIndexStore(dataSource, block, descriptorsByPath, parquetTupleDomain, parquetReaderOptions);
                    Optional<BloomFilterStore> bloomFilterStore = getBloomFilterStore(dataSource, block, parquetTupleDomain, parquetReaderOptions);
                    PrunedBlockMetadata prunedBlockMetadata = createPrunedColumnsMetadata(block, dataSource.getId(), descriptorsByPath);

                    if (start <= firstDataPage && firstDataPage < start + length
                            && predicateMatches(parquetPredicate, prunedBlockMetadata, dataSource, descriptorsByPath, parquetTupleDomain, columnIndex, bloomFilterStore, UTC, domainCompactionThreshold)) {
                        rowGroups.add(new RowGroupInfo(
                                prunedBlockMetadata,
                                nextStart,
                                columnIndex));
                    }
                    nextStart += block.rowCount();
                }

                Optional<ReaderColumns> readerProjections = projectBaseColumns(columnHandles, useColumnNames);
                List<DviewColumnHandle> baseColumns = readerProjections.map(projection ->
                                projection.get().stream()
                                        .map(DviewColumnHandle.class::cast)
                                        .toList())
                        .orElse(columnHandles);
                ParquetPageSource.Builder pageSourceBuilder = ParquetPageSource.builder();
                ImmutableList.Builder<Column> parquetColumnFieldsBuilder = ImmutableList.builder();
                int sourceChannel = 0;
                for (DviewColumnHandle column : baseColumns) {
                    if (Objects.equals(column.getColumnName(), PARQUET_ROW_INDEX_COLUMN.getColumnName())) {
                        pageSourceBuilder.addRowIndexColumn();
                    }
                    else if (column.getColumnName().equals(dviewConfig.getPartitionFirst()) && partitionDate != null) {
                        pageSourceBuilder.addConstantColumn(
                                nativeValueToBlock(column.getColumnType(), partitionDate.getTime() / 86400000));
                    }
                    else if (column.getColumnName().equals(dviewConfig.getPartitionSecond()) && partitionTime != null) {
                        pageSourceBuilder.addConstantColumn(
                                nativeValueToBlock(column.getColumnType(), partitionTime.getTime()));
                    }
                    else {
                        Optional<org.apache.parquet.schema.Type> parquetType = getBaseColumnParquetType(column, fileSchema, useColumnNames);
                        if (parquetType.isEmpty()) {
                            pageSourceBuilder.addNullColumn(column.getColumnType());
                            continue;
                        }
                        String columnName = useColumnNames ? column.getColumnName() : fileSchema.getFields().get(column.getOrdinalPosition()).getName();
                        Optional<Field> field = constructField(column.getColumnType(), lookupColumnByName(messageColumn, columnName));
                        if (field.isEmpty()) {
                            pageSourceBuilder.addNullColumn(column.getColumnType());
                            continue;
                        }
                        parquetColumnFieldsBuilder.add(new Column(columnName, field.get()));
                        pageSourceBuilder.addSourceColumn(sourceChannel);
                        sourceChannel++;
                    }
                }

                ParquetReader parquetReader = new ParquetReader(
                        Optional.ofNullable(fileMetaData.getCreatedBy()),
                        parquetColumnFieldsBuilder.build(),
                        rowGroups.build(),
                        dataSource,
                        dateTimeZone,
                        newSimpleAggregatedMemoryContext(),
                        parquetReaderOptions,
                        exception -> DviewParquetPageSource.handleException(dataSource.getId(), exception),
                        Optional.of(parquetPredicate),
                        Optional.empty());

                return pageSourceBuilder.build(parquetReader);
            }
            catch (FileNotFoundException fileNotFoundException) {
                int sleepMs = retryCount * sleepIntervalMs;
                log.error("File not found, File maybe deleted for overwrite, retrying in {0} ms", sleepMs);
                Thread.sleep(sleepMs);
                lastErrorEncountered = fileNotFoundException;
            }
            catch (IOException ioException) {
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
        log.info("Entering into DviewPageSourceProvider::getParquetTupleDomain");
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        for (Map.Entry<DviewColumnHandle, Domain> entry : effectivePredicate.getDomains().get().entrySet()) {
            DviewColumnHandle columnHandle = entry.getKey();

            ColumnDescriptor descriptor = null;

            Optional<org.apache.parquet.schema.Type> baseColumnType = getBaseColumnParquetType(columnHandle, fileSchema, useColumnNames);
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
            BlockMetadata blockMetadata,
            Map<List<String>, ColumnDescriptor> descriptorsByPath,
            TupleDomain<ColumnDescriptor> parquetTupleDomain,
            ParquetReaderOptions options)
    {
        if (!options.isUseColumnIndex() || parquetTupleDomain.isAll() || parquetTupleDomain.isNone()) {
            return Optional.empty();
        }

        boolean hasColumnIndex = false;
        for (ColumnChunkMetadata column : blockMetadata.columns()) {
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

    public static Optional<MessageType> getParquetMessageType(List<DviewColumnHandle> columns, boolean useColumnNames, MessageType fileSchema)
    {
        return Optional.of(fileSchema);
    }

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

        for (HiveColumnHandle columnHandle : columns) {
            DereferenceChain column = dereferenceChains.inverse().get(columnHandle);
            List<DereferenceChain> orderedPrefixes = column.getOrderedPrefixes();
            DereferenceChain chosenColumn = null;

            for (DereferenceChain prefix : orderedPrefixes) {
                if (dereferenceChains.containsKey(prefix)) {
                    chosenColumn = prefix;
                    break;
                }
            }

            checkState(chosenColumn != null, "chosenColumn is null");
            int inputBlockIndex;

            if (pickedColumns.containsKey(chosenColumn)) {
                inputBlockIndex = pickedColumns.get(chosenColumn);
            }
            else {
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

    public static Optional<ReaderColumns> projectBaseColumns(List<DviewColumnHandle> columns, boolean useColumnNames)
    {
        requireNonNull(columns, "columns is null");
        return Optional.empty();
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
