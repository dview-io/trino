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
import io.dview.schema.fortress.models.schema.entity.Entity;
import io.dview.schema.fortress.models.schema.meta.CloudProvider;
import io.trino.plugin.dview.DviewConfig;
import io.trino.plugin.dview.table.DviewTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class DviewClient
{
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private static final Logger log = Logger.get(DviewClient.class);
    private final Supplier<Map<String, Map<String, DviewTable>>> schemas;
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
                Map<String, DviewTable> tables = entities.parallelStream().collect(Collectors.toMap((Entity::getName), (entity -> new DviewTable(entity, entity.getCurrentSchema()))));
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
}
