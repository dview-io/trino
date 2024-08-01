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
package io.trino.plugin.dview.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.dview.schema.fortress.models.schema.entity.Attribute;
import io.trino.plugin.dview.client.DviewClient;
import io.trino.plugin.dview.table.DviewInsertTableHandle;
import io.trino.plugin.dview.table.DviewOutputTableHandle;
import io.trino.plugin.dview.table.DviewTable;
import io.trino.plugin.dview.table.DviewTableHandle;
import io.trino.plugin.dview.table.column.DviewColumnHandle;
import io.trino.plugin.dview.utils.AttributeUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.statistics.ComputedStatistics;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class DviewConnectorMetadata
        implements ConnectorMetadata
{
    private final DviewClient dviewClient;
    private static final Logger log = Logger.get(DviewConnectorMetadata.class);

    private final Set<String> tableNamesSet = new HashSet<>();

    @Inject
    public DviewConnectorMetadata(DviewClient client)
    {
        this.dviewClient = client;
    }

    public List<String> listSchemaNames()
    {
        return ImmutableList.copyOf(dviewClient.getSchemaNames());
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        log.info("Entering into DviewConnectorMetadata::getTableMetadata");
        return getTableMetadata(((DviewTableHandle) table).toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        log.info("Entering into DviewConnectorMetadata::listTables");
        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(dviewClient.getSchemaNames()));

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : dviewClient.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public DviewTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        log.info("Entering into DviewConnectorMetadata::getTableHandle");
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }
        DviewTable table = dviewClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }
        return new DviewTableHandle(tableName.getSchemaName(), table.getEntity().getCloudProvider().getName().toString(), table.getEntity(), table.getEntitySchema(), table.getColumns(), table.getEntity().getCloudProvider().getConfigs(), table.getEntityId());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        log.info("Entering into DviewConnectorMetadata::getColumnHandles");
        DviewTableHandle exampleTableHandle = (DviewTableHandle) tableHandle;

        DviewTable table = dviewClient.getTable(exampleTableHandle.getSchemaName(), exampleTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(exampleTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (Attribute column : table.getEntitySchema().getAttributes()) {
            columnHandles.put(column.getName(), new DviewColumnHandle(
                    column.getName(),
                    AttributeUtils.convertFortressTypeToTrinoType(column.getAttributeType().getDatatype()),
                    index,
                    column.isPartitionKey(),
                    column.getDefaultValue()));
            index++;
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        log.info("Entering into DviewConnectorMetadata::listTableColumns");
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.buildOrThrow();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        log.info("Entering into getTableMetadata");
        tableNamesSet.add(String.valueOf(tableName));
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }
        DviewTable table = dviewClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }
        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata(), table.getProperties());
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        log.info("Entering into listTables of DviewConnectorMetadata");
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        log.info("Entering into DviewConnectorMetadata::getColumnMetadata");
        return ((DviewColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        log.info("Entering into DviewConnectorMetadata::createTable");
        dviewClient.createTable(tableMetadata);
        dviewClient.refresh();
    }

    @Override
    public DviewInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        log.info("Entering into DviewConnectorMetadata::beginInsert");
        SchemaTableName tableName = ((DviewTableHandle) tableHandle).toSchemaTableName();
        String parentTableName = "";
        for (String name : tableNamesSet) {
            if (!name.equals(String.valueOf(tableName))) {
                parentTableName = name;
                break;
            }
        }
        dviewClient.insertTable(tableHandle, columns, parentTableName);
        return new DviewInsertTableHandle(
                ((DviewTableHandle) tableHandle).getSchemaName(),
                ((DviewTableHandle) tableHandle).getCloudProvider(),
                ((DviewTableHandle) tableHandle).getEntity(),
                ((DviewTableHandle) tableHandle).getEntitySchema(),
                ((DviewTableHandle) tableHandle).getColumns(),
                ((DviewTableHandle) tableHandle).getConfigurations(),
                ((DviewTableHandle) tableHandle).getEntityId(),
                ((DviewTableHandle) tableHandle).getEntity().getBasePath());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        log.info("Entering into DviewConnectorMetadata::finishInsert");
        return Optional.empty();
    }

    @Override
    public DviewOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        log.info("Entering into DviewConnectorMetadata::beginCreateTable");
        dviewClient.refresh();
        SchemaTableName tableName = tableMetadata.getTable();

        dviewClient.beginCreateTable(session, tableMetadata);

        DviewTableHandle tableHandle = getTableHandle(session, tableName);

        return new DviewOutputTableHandle(
                (tableHandle).getSchemaName(),
                (tableHandle).getCloudProvider(),
                (tableHandle).getEntity(),
                (tableHandle).getEntitySchema(),
                (tableHandle).getColumns(),
                (tableHandle).getConfigurations(),
                (tableHandle).getEntityId(),
                (tableHandle).getEntity().getBasePath());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        log.info("Entering into DviewConnectorMetadata::finishCreateTable");
        return Optional.empty();
    }
}
