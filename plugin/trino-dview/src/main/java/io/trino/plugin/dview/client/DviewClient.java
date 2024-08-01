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
package io.trino.plugin.dview.client;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.dview.schema.fortress.client.client.FortressClient;
import io.dview.schema.fortress.client.client.FortressClientFactory;
import io.dview.schema.fortress.client.contracts.DocumentContract;
import io.dview.schema.fortress.client.contracts.EntityContract;
import io.dview.schema.fortress.client.contracts.SchemaContract;
import io.dview.schema.fortress.client.exception.FortressClientNotFound;
import io.dview.schema.fortress.models.schema.Catalog;
import io.dview.schema.fortress.models.schema.Namespace;
import io.dview.schema.fortress.models.schema.Tenant;
import io.dview.schema.fortress.models.schema.entity.Attribute;
import io.dview.schema.fortress.models.schema.entity.Entity;
import io.dview.schema.fortress.models.schema.entity.properties.AttributeType;
import io.dview.schema.fortress.models.schema.file.Segment;
import io.dview.schema.fortress.models.schema.meta.CloudProvider;
import io.trino.plugin.dview.DviewConfig;
import io.trino.plugin.dview.table.DviewTable;
import io.trino.plugin.dview.table.DviewTableHandle;
import io.trino.plugin.dview.utils.DviewCreateTableUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static java.util.Objects.requireNonNull;

public class DviewClient
{
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private static final Logger log = Logger.get(DviewClient.class);
    private Supplier<Map<String, Map<String, DviewTable>>> schemas;
    private final FortressClient client;
    private final Tenant tenant;
    private final SchemaContract schemaContract;
    private final EntityContract entityContract;
    private final DocumentContract documentContract;
    private final Namespace namespace;

    public EntityContract getEntityContract()
    {
        return this.entityContract;
    }

    public SchemaContract getSchemaContract()
    {
        return this.schemaContract;
    }

    public DocumentContract getDocumentContract()
    {
        return this.documentContract;
    }

    @Inject
    public DviewClient(DviewConfig config)
    {
        try {
            this.client = FortressClientFactory.getFortressClient(config.getFortressClientConfig());
            this.schemaContract = this.client.getSchemaContract();
            this.tenant = getTenant(config.getOrg(), config.getTenant());
            this.namespace = getNamespace(tenant, config.getNamespace());
            this.entityContract = this.client.getEntityContract();
            this.documentContract = this.client.getDocumentContract();
            this.schemas = Suppliers.memoize(schemasSupplier());
        }
        catch (FortressClientNotFound e) {
            throw new RuntimeException(e);
        }
    }

    public void refresh()
    {
        this.schemas = Suppliers.memoize(schemasSupplier());
    }

    public Set<String> getSchemaNames()
    {
        return schemas.get().keySet();
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        Map<String, DviewTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public DviewTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, DviewTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    private Supplier<Map<String, Map<String, DviewTable>>> schemasSupplier()
    {
        return () -> {
            Map<String, Map<String, DviewTable>> databaseMap = new HashMap<>();
            for (Catalog catalog : namespace.getCatalogs()) {
                List<Entity> entities = getEntityContract().getAllEntities(catalog);
                Map<String, DviewTable> tables = entities.parallelStream().collect(Collectors.toMap((Entity::getName), (entity -> new DviewTable(entity, entity.getCurrentSchema(), new HashMap<>()))));
                databaseMap.put(catalog.getName(), tables);
            }
            return databaseMap;
        };
    }

    private Tenant getTenant(String org, String tenantName)
            throws FortressClientNotFound
    {
        try {
            Tenant tenant = getSchemaContract().getTenantBy(org, tenantName);
            log.info("Running trino for org: {} and tenant: {}", tenant.getOrg(), tenant.getName());
            return tenant;
        }
        catch (FortressClientNotFound fortressClientNotFound) {
            log.error("Tenant not found with org {} and name {}", org, tenantName);
            throw fortressClientNotFound;
        }
    }

    private Namespace getNamespace(Tenant tenant, String name)
            throws FortressClientNotFound
    {
        try {
            Namespace namespace = getSchemaContract().getNamespaceByName(tenant, name);
            log.info("Running Trino for namespace: {0} and tenant {1}", namespace.getName(), tenant.getName());
            return namespace;
        }
        catch (FortressClientNotFound fortressClientNotFound) {
            log.error("Namespace not found with tenant {0} and name {1}", tenant.getName(), name);
            throw fortressClientNotFound;
        }
    }

    public CloudProvider getCloudProviderFor(long entityId)
    {
        Entity entity = Entity.builder().id(entityId).build();
        return client.getEntityContract().getCloudProviderFor(entity);
    }

    public void createTable(ConnectorTableMetadata tableMetadata)
    {
        log.info("Entering into DviewClient::createTable");
        try {
            SchemaTableName tableName = tableMetadata.getTable();
            List<ColumnMetadata> columns = tableMetadata.getColumns();
            Map<String, Object> properties = tableMetadata.getProperties();
            long catalogId = client.getSchemaContract().upsertCatalog(tableName.getSchemaName(), "default", tenant.getOrg(), tenant.getName());
            Entity entity = DviewCreateTableUtils.insertEntryIntoTable(tableName.getTableName(), catalogId, properties);
            long entityId = client.getEntityContract().upsertEntity(entity);
            long entitySchemaId = client.getEntityContract().upsertEntitySchema(entityId, null, null, null);
            List<Attribute> attributes = DviewCreateTableUtils.insertIntoAttribute(columns);
            boolean result = client.getSchemaContract().insertAttribute(attributes, entitySchemaId);
            client.getDocumentContract().insertIntoSegmentAndDocument(entityId, entitySchemaId, properties, true);
        }
        catch (SQLException | FortressClientNotFound e) {
            log.error("Error while creating the table: {}", e);
            throw new TrinoException(UNSUPPORTED_TABLE_TYPE, "Table creation failed for: " + tableMetadata.getTable().getTableName() + e.getMessage());
        }
        log.info("Table ADDED SUCCESSFULLY");
    }

    public void insertTable(ConnectorTableHandle tableHandle, List<ColumnHandle> columns, String tableName)
    {
        log.info("Entering into DviewClient::insertTable");

        if (tableName.isEmpty()) {
            throw new TrinoException(NOT_FOUND, "tableName not found in insertTable");
        }

        String newTableName = tableName;

        if (tableName.contains(".")) {
            String[] parts = tableName.split("\\.");
            newTableName = parts[1];
        }
        long entityId = client.getEntityContract().getEntityIdByName(newTableName);
        long entitySchemaId = client.getEntityContract().getEntitySchemaId(entityId);
        List<Attribute> attributes = client.getEntityContract().getAttributesByEntitySchemaId(entitySchemaId);

        // Extract attributes from tableHandle
        DviewTableHandle dviewTableHandle = (DviewTableHandle) tableHandle;
        List<Attribute> tableAttributes = dviewTableHandle.getEntity().getCurrentSchema().getAttributes();

        // Compare attributes
        if (attributes.size() != tableAttributes.size()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Attribute count mismatch");
        }

        for (int i = 0; i < attributes.size(); i++) {
            Attribute fetchedAttribute = attributes.get(i);
            Attribute tableAttribute = tableAttributes.get(i);

            if (!fetchedAttribute.getName().equals(tableAttribute.getName())) {
                System.out.println("fetchedAttributeName: " + fetchedAttribute.getName());
                System.out.println("tableAttributeName: " + tableAttribute.getName());
                throw new TrinoException(INVALID_TABLE_PROPERTY, "Attribute name mismatch at position " + i);
            }
            if (!fetchedAttribute.getAttributeType().getDatatype().equals(AttributeType.Type.FLOAT) && !fetchedAttribute.getAttributeType().getDatatype().equals(AttributeType.Type.DOUBLE)) {
                if (!fetchedAttribute.getAttributeType().getDatatype().equals(tableAttribute.getAttributeType().getDatatype())) {
                    System.out.println("fetchedAttributeDataType: " + fetchedAttribute.getAttributeType().getDatatype());
                    System.out.println("tableAttributeDataType: " + tableAttribute.getAttributeType().getDatatype());
                    throw new TrinoException(INVALID_TABLE_PROPERTY, "Attribute datatype mismatch at position " + i);
                }
            }

            if (fetchedAttribute.getOrdinalPosition() != tableAttribute.getOrdinalPosition()) {
                System.out.println("fetchedAttributeOrdinalPosition: " + fetchedAttribute.getOrdinalPosition());
                System.out.println("tableAttributeOrdinalPosition: " + tableAttribute.getOrdinalPosition());
                throw new TrinoException(INVALID_TABLE_PROPERTY, "Attribute ordinal position mismatch at position " + i);
            }
        }
    }

    public void beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        log.info("Entering into DviewClient::beginCreateTable");

        try {
            SchemaTableName tableName = tableMetadata.getTable();
            List<ColumnMetadata> columns = tableMetadata.getColumns();
            Map<String, Object> properties = tableMetadata.getProperties();
            String schemaName = tableName.getSchemaName();
            String tableNameStr = tableName.getTableName();

            long catalogId = client.getSchemaContract().upsertCatalog(schemaName, "default", tenant.getOrg(), tenant.getName());

            Entity entity = DviewCreateTableUtils.insertEntryIntoTable(tableNameStr, catalogId, properties);
            long entityId = client.getEntityContract().upsertEntity(entity);

            long entitySchemaId = client.getEntityContract().upsertEntitySchema(entityId, null, null, null);

            List<Attribute> attributes = DviewCreateTableUtils.insertIntoAttribute(columns);

            client.getSchemaContract().insertAttribute(attributes, entitySchemaId);

            client.getDocumentContract().insertIntoSegmentAndDocument(entityId, entitySchemaId, properties, false);
        }
        catch (SQLException | FortressClientNotFound e) {
            log.error("Error in DviewClient::beginCreateTable while creating the table: {}", e);
            throw new TrinoException(UNSUPPORTED_TABLE_TYPE, "Table creation failed for: " + tableMetadata.getTable().getTableName() + e.getMessage());
        }
    }

    public void insertIntoDocument(Set<String> outputFilePaths, long entityId)
    {
        log.info("Entering into DviewClient::insertIntoDocument");

        List<Segment> segments = client.getDocumentContract().getSegmentsByEntityId(entityId);
        long segmentId = segments.get(0).getId();

        for (String path : outputFilePaths) {
            int lastSlashIndex = path.lastIndexOf('/');
            String fileName = path.substring(lastSlashIndex + 1);

            client.getDocumentContract().insertDocument(entityId, segmentId, path, fileName, null, null);
        }
    }
}
