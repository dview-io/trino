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
import io.trino.spi.connector.ConnectorInsertTableHandle;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class DviewInsertTableHandle
        extends DviewTableHandle
        implements ConnectorInsertTableHandle
{
    private final String filePath;

    @JsonCreator
    public DviewInsertTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("cloudProvider") String cloudProvider,
            @JsonProperty("entity") Entity entity,
            @JsonProperty("entitySchema") EntitySchema entitySchema,
            @JsonProperty("columns") List<DviewColumn> columns,
            @JsonProperty("configurations") Map<String, Object> configurations,
            @JsonProperty("entityId") long entityId,
            @JsonProperty("filePath") String filePath)
    {
        super(schemaName, cloudProvider, entity, entitySchema, columns, configurations, entityId);
        this.filePath = requireNonNull(filePath, "filePath is null");
    }

    @JsonProperty
    public String getFilePath()
    {
        return filePath;
    }

    @Override
    public String toString()
    {
        return "DviewInsertTableHandle{" +
                "schemaName='" + getSchemaName() + '\'' +
                ", entity=" + getEntity() +
                ", entitySchema=" + getEntitySchema() +
                ", cloudProvider='" + getCloudProvider() + '\'' +
                ", columns=" + getColumns() +
                ", configurations=" + getConfigurations() +
                ", entityId=" + getEntityId() +
                ", filePath='" + filePath + '\'' +
                '}';
    }
}
