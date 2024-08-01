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
import com.google.common.collect.ImmutableList;
import io.dview.schema.fortress.models.schema.entity.Entity;
import io.dview.schema.fortress.models.schema.entity.EntitySchema;
import io.trino.plugin.dview.table.column.DviewColumn;
import io.trino.plugin.dview.utils.AttributeUtils;
import io.trino.spi.connector.ColumnMetadata;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class DviewTable
{
    private final String name;
    private final List<DviewColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;
    private final Entity entity;
    private final long entityId;
    private final EntitySchema entitySchema;
    private final Map<String, Object> properties;

    @JsonCreator
    public DviewTable(@JsonProperty("entity") Entity entity, @JsonProperty("entitySchema") EntitySchema entitySchema, @JsonProperty("properties") Map<String, Object> properties)
    {
        checkArgument(!isNullOrEmpty(entity.getName()), "name is null or is empty");
        this.name = requireNonNull(entity.getName(), "name is null");
        this.columns = entitySchema.getAttributes().stream()
                .map((attribute ->
                        new DviewColumn(attribute.getName(),
                                AttributeUtils.convertFortressTypeToTrinoType(attribute.getAttributeType().getDatatype()),
                                attribute.getOrdinalPosition())))
                .collect(Collectors.toList());

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (DviewColumn column : this.columns) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType()));
        }
        this.columnsMetadata = columnsMetadata.build();
        this.entity = entity;
        this.entitySchema = entitySchema;
        this.entityId = entity.getId();
        this.properties = properties;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<DviewColumn> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }

    @JsonProperty
    public Entity getEntity()
    {
        return entity;
    }

    @JsonProperty
    public long getEntityId()
    {
        return entityId;
    }

    @JsonProperty
    public EntitySchema getEntitySchema()
    {
        return entitySchema;
    }

    @JsonProperty
    public Map<String, Object> getProperties()
    {
        return properties;
    }
}
