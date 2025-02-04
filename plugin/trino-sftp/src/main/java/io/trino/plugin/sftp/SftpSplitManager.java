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
package io.trino.plugin.sftp;

import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SftpSplitManager
        implements ConnectorSplitManager
{
    private final SftpFileManager fileManager;
    private final SchemaStore schemaStore;
    private final NodeManager nodeManager;

    public SftpSplitManager(SftpFileManager fileManager, SchemaStore schemaStore, NodeManager nodeManager)
    {
        this.fileManager = fileManager;
        this.schemaStore = schemaStore;
        this.nodeManager = nodeManager;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        SftpTableHandle tableHandle = (SftpTableHandle) table;
        TableMetadata metadata = schemaStore.getTableMetadata(tableHandle.getSchemaTableName());
        try {
            List<SftpFile> files = fileManager.listFiles(
                    metadata.getDirectoryPath(),
                    metadata.getCompressionType());
            List<ConnectorSplit> splits = new ArrayList<>();
            for (SftpFile file : files) {
                splits.add(new SftpSplit(
                        file.getPath(),
                        file.getSize(),
                        0,
                        file.getSize(),
                        nodeManager.getRequiredWorkerNodes().stream().map(Node::getHostAndPort).toList()));
            }

            return new FixedSplitSource(splits);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
