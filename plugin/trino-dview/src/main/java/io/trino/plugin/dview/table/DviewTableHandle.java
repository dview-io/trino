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
package io.trino.plugin.dview.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dview.schema.fortress.models.schema.entity.Entity;
import io.dview.schema.fortress.models.schema.entity.EntitySchema;
import io.trino.plugin.dview.table.column.DviewColumn;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class DviewTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final Entity entity;
    private final EntitySchema entitySchema;
    private final String cloudProvider;
    private final List<DviewColumn> columns;
    private final Map<String, Object> configurations;

    @JsonCreator
    public DviewTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("cloudProvider") String cloudProvider,
            @JsonProperty("entity") Entity entity,
            @JsonProperty("entitySchema") EntitySchema entitySchema,
            @JsonProperty("columns") List<DviewColumn> columns,
            @JsonProperty("configurations") Map<String, Object> configurations)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.entity = requireNonNull(entity, "entity is null");
        this.entitySchema = requireNonNull(entitySchema, "tableName is null");
        this.cloudProvider = requireNonNull(cloudProvider, "CloudProvider can't be null");
        this.columns = requireNonNull(columns, "No Columns provided null");
        this.configurations = requireNonNull(configurations, "No Configurations provided");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return entity.getName();
    }

    @JsonProperty
    public Entity getEntity()
    {
        return entity;
    }

    @JsonProperty
    public EntitySchema getEntitySchema()
    {
        return entitySchema;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, entity.getName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, entity.getName());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        DviewTableHandle other = (DviewTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.entity, other.entity);
    }

    @JsonProperty
    public String getCloudProvider()
    {
        return cloudProvider;
    }

    @JsonProperty
    public List<DviewColumn> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Map<String, Object> getConfigurations()
    {
        return configurations;
    }

    @Override
    public String toString()
    {
        return "DviewTableHandle{" +
                "schemaName='" + schemaName + '\'' +
                ", entity=" + entity +
                ", entitySchema=" + entitySchema +
                ", cloudProvider='" + cloudProvider + '\'' +
                ", columns=" + columns +
                ", configurations=" + configurations +
                '}';
    }
}
