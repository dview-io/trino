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

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import java.util.List;

public class SftpRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final SftpFileManager fileManager;
    private final SchemaStore schemaStore;

    public SftpRecordSetProvider(SftpFileManager fileManager, SchemaStore schemaStore)
    {
        this.fileManager = fileManager;
        this.schemaStore = schemaStore;
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> columns)
    {
        SftpSplit sftpSplit = (SftpSplit) split;
        SftpTableHandle tableHandle = (SftpTableHandle) table;

        TableMetadata metadata = schemaStore.getTableMetadata(tableHandle.getSchemaTableName());
        List<ColumnMetadata> columnMetadata = metadata.getColumns();

        return new SftpRecordSet(
                fileManager,
                sftpSplit.getFilePath(),
                sftpSplit.getFileSize(),
                columns,
                columnMetadata,
                metadata.getCompressionType(),
                metadata.getDelimiter().charAt(0),
                metadata.isSkipHeader());
    }
}
