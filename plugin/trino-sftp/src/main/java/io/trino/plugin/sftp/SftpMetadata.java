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

import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TypeManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SftpMetadata
        implements ConnectorMetadata
{
    private final SchemaStore schemaStore;
    private final TypeManager typeManager;
    private final Logger log = Logger.get(SftpMetadata.class);

    public SftpMetadata(SchemaStore schemaStore, TypeManager typeManager)
    {
        this.schemaStore = schemaStore;
        this.typeManager = typeManager;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        System.out.println("Inside SftpMetadata.listTables");
        log.info("Inside SftpMetadata.listTables");
        return schemaStore.listTables(schemaName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        System.out.println("Inside SftpMetadata.getTableMetadata");
        log.info("Inside SftpMetadata.getTableMetadata");
        SftpTableHandle sftpTableHandle = (SftpTableHandle) tableHandle;
        TableMetadata metadata = schemaStore.getTableMetadata(sftpTableHandle.getSchemaTableName());
        return metadata.toConnectorTableMetadata();
    }

    @Override
    public SftpTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        System.out.println("Inside SftpMetadata.getTableHandle");
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            System.out.println("Inside SftpMetadata.getTableHandle");
            return null;
        }
        try {
            return schemaStore.getTableMetadata(tableName).toTableHandle();
        }
        catch (TableNotFoundException e) {
            return null;
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        System.out.println("Inside SftpMetadata.getColumnMetadata");
        log.info("Inside SftpMetadata.getColumnMetadata");
        SftpColumnHandle sftpColumnHandle = (SftpColumnHandle) columnHandle;
        return new ColumnMetadata(
                sftpColumnHandle.getName(),
                sftpColumnHandle.getType());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        System.out.println("Inside SftpMetadata.getColumnHandles");
        log.info("Inside SftpMetadata.getColumnHandles");
        SftpTableHandle sftpTableHandle = (SftpTableHandle) tableHandle;
        TableMetadata metadata = schemaStore.getTableMetadata(sftpTableHandle.getSchemaTableName());
        Map<String, ColumnHandle> columnHandles = new HashMap<>();
        List<ColumnMetadata> columns = metadata.getColumns();

        for (int i = 0; i < columns.size(); i++) {
            ColumnMetadata column = columns.get(i);
            columnHandles.put(column.getName(), new SftpColumnHandle(
                    column.getName(),
                    column.getType(),
                    i));
        }

        return columnHandles;
    }

    @Override
    public void createTable(
            ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            boolean ignoreExisting)
    {
        System.out.println("Inside SftpMetadata.createTable");
        log.info("Inside SftpMetadata.createTable");
        SchemaTableName tableName = tableMetadata.getTable();
        Map<String, Object> properties = tableMetadata.getProperties();

        // Validate required properties
        requireNonNull(properties.get("directory_path"), "directory_path is required");

        // Store table metadata
        schemaStore.storeTableSchema(
                tableName.getSchemaName(),
                tableName.getTableName(),
                tableMetadata.getColumns(),
                properties);
    }

    @Override
    public void createSchema(
            ConnectorSession session,
            String schemaName,
            Map<String, Object> properties,
            TrinoPrincipal owner)
    {
        System.out.println("Inside SftpMetadata.session");
        log.info("Inside SftpMetadata.session");
        schemaStore.createSchema(schemaName);
    }

    @Override
    public void dropTable(
            ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        SftpTableHandle sftpTableHandle = (SftpTableHandle) tableHandle;
        schemaStore.dropTable(sftpTableHandle.getSchemaTableName());
    }

    @Override
    public void dropSchema(
            ConnectorSession session,
            String schemaName,
            boolean cascade)
    {
        if (cascade) {
            // Implement cascade logic to drop all tables first
            // dropTablesInSchema(schemaName);
            throw new IllegalArgumentException("cascade is not supported");
        }
        schemaStore.dropSchema(schemaName);
    }

    @Override
    public void renameTable(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            SchemaTableName newTableName)
    {
        SftpTableHandle sftpTableHandle = (SftpTableHandle) tableHandle;
        schemaStore.renameTable(
                sftpTableHandle.getSchemaTableName(),
                newTableName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return schemaStore.listSchemaNames();
    }
}
