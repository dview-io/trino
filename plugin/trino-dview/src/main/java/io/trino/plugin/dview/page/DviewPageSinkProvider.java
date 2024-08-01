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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.dview.client.DviewClient;
import io.trino.plugin.dview.table.DviewInsertTableHandle;
import io.trino.plugin.dview.table.DviewOutputTableHandle;
import io.trino.plugin.dview.table.column.DviewColumn;
import io.trino.plugin.dview.utils.FileUtils;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

public class DviewPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final DviewClient dviewClient;
    private static final Logger log = Logger.get(DviewPageSinkProvider.class);
    private TrinoFileSystem fileSystem;

    @Inject
    public DviewPageSinkProvider(DviewClient dviewClient)
    {
        this.dviewClient = dviewClient;
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle,
            ConnectorPageSinkId pageSinkId)
    {
        log.info("Entering into createPageSink for insertTableHandle");

        DviewOutputTableHandle handle = (DviewOutputTableHandle) outputTableHandle;
        Location location = Location.of(handle.getFilePath());
        long entityId = handle.getEntityId();

        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(
                new HdfsEnvironment(
                        new DynamicHdfsConfiguration(
                                new HdfsConfigurationInitializer(
                                        new HdfsConfig(),
                                        Set.of(FileUtils.getConfigurationInitializerForCloudProvider(handle.getFilePath(), dviewClient.getCloudProviderFor(handle.getEntityId())))),
                                ImmutableSet.of()),
                        new HdfsConfig(),
                        new NoHdfsAuthentication()),
                new TrinoHdfsFileSystemStats());

        fileSystem = fileSystemFactory.create(session);
        TrinoOutputFile outputFile = fileSystem.newOutputFile(location);
        List<String> fileColumnNames = handle.getColumns().stream()
                .map(DviewColumn::getName)
                .toList();

        List<Type> fileColumnTypes = handle.getColumns().stream()
                .map(DviewColumn::getType)
                .toList();

        List<Long> columnOrdinalPositions = handle.getColumns().stream()
                .map(DviewColumn::getOrdinalPosition)
                .toList();

        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                fileColumnTypes,
                fileColumnNames,
                true,
                true);
        return getDviewPageSink(session, location, outputFile, fileColumnNames, fileColumnTypes, schemaConverter, entityId);
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle,
            ConnectorPageSinkId pageSinkId)
    {
        log.info("Entering into createPageSink for insertTableHandle");
        DviewInsertTableHandle handle = (DviewInsertTableHandle) insertTableHandle;
        Location location = Location.of(handle.getFilePath());
        long entityId = handle.getEntityId();

        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(
                new HdfsEnvironment(
                        new DynamicHdfsConfiguration(
                                new HdfsConfigurationInitializer(
                                        new HdfsConfig(),
                                        Set.of(FileUtils.getConfigurationInitializerForCloudProvider(handle.getFilePath(), dviewClient.getCloudProviderFor(handle.getEntityId())))),
                                ImmutableSet.of()),
                        new HdfsConfig(),
                        new NoHdfsAuthentication()),
                new TrinoHdfsFileSystemStats());

        fileSystem = fileSystemFactory.create(session);
        TrinoOutputFile outputFile = fileSystem.newOutputFile(location);
        List<String> fileColumnNames = handle.getColumns().stream()
                .map(DviewColumn::getName)
                .toList();

        List<Type> fileColumnTypes = handle.getColumns().stream()
                .map(DviewColumn::getType)
                .toList();

        List<Long> columnOrdinalPositions = handle.getColumns().stream()
                .map(DviewColumn::getOrdinalPosition)
                .toList();

        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(
                fileColumnTypes,
                fileColumnNames,
                true,
                true);
        return getDviewPageSink(session, location, outputFile, fileColumnNames, fileColumnTypes, schemaConverter, entityId);
    }

    private DviewPageSink getDviewPageSink(ConnectorSession session, Location location, TrinoOutputFile outputFile, List<String> fileColumnNames, List<Type> fileColumnTypes, ParquetSchemaConverter schemaConverter, long entityId)
    {
        log.info("Entering into DviewPageSinkProvider::getDviewPageSink");
        Closeable rollbackAction = () -> fileSystem.deleteFile(location);

        HiveCompressionCodec compressionCodec = HiveCompressionCodec.NONE;
        String trinoVersion = session.getUser();
        ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                .build();

        DviewPageSink pageSink;
        try {
            pageSink = new DviewPageSink(
                    dviewClient,
                    fileSystem,
                    outputFile,
                    rollbackAction,
                    fileColumnTypes,
                    fileColumnNames,
                    schemaConverter.getMessageType(),
                    schemaConverter.getPrimitiveTypes(),
                    parquetWriterOptions,
                    IntStream.range(0, fileColumnNames.size()).toArray(),
                    compressionCodec.getParquetCompressionCodec(),
                    trinoVersion,
                    entityId);
        }
        catch (IOException e) {
            log.error("Error while calling getDviewPageSink");
            throw new RuntimeException(e);
        }
        return pageSink;
    }
}
