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
package io.trino.plugin.dview.utils;

import com.sun.jdi.FloatType;
import io.airlift.log.Logger;
import io.dview.schema.fortress.models.schema.Catalog;
import io.dview.schema.fortress.models.schema.entity.Attribute;
import io.dview.schema.fortress.models.schema.entity.Entity;
import io.dview.schema.fortress.models.schema.entity.properties.AttributeType;
import io.dview.schema.fortress.models.schema.meta.CloudProvider;
import io.dview.schema.fortress.models.schema.meta.StorageFormat;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DviewCreateTableUtils
{
    private static final Logger log = Logger.get(DviewCreateTableUtils.class);

    private DviewCreateTableUtils()
    {
    }

    public static Entity insertEntryIntoTable(String tableName, long catalogId, Map<String, Object> properties)
    {
        log.info("Entering into DviewCreateTableUtils::getEntityForInsertTable");
        Entity entity = new Entity();

        List<String> identityKeys = new ArrayList<>();
        identityKeys.add("id");
        entity.setIdentityKeys(identityKeys);

        StorageFormat storageFormat = new StorageFormat();
        storageFormat.setFormat(StorageFormat.Format.valueOf("PARQUET"));
        entity.setStorageFormat(storageFormat);

        CloudProvider cloudProvider = new CloudProvider();
        cloudProvider.setName(CloudProvider.Provider.valueOf("S3"));
        entity.setCloudProvider(cloudProvider);

        Catalog catalog = new Catalog();
        catalog.setId(catalogId);
        entity.setCatalog(catalog);

        entity.setName(tableName);
        entity.setBasePath(properties.get("external_location").toString());
        entity.setHourPartitioned(true);
        entity.setDescription("Table " + tableName + " entry made");
        entity.setFileRetainCount((byte) 3);
        entity.setIdentityKeys(identityKeys);
        return entity;
    }

    public static List<Attribute> insertIntoAttribute(List<ColumnMetadata> columns)
    {
        log.info("Entering into DviewCreateTableUtils::insertIntoAttribute");
        List<Attribute> attributes = new ArrayList<>();
        int ordinalPosition = 1;

        for (ColumnMetadata column : columns) {
            AttributeType trinoType = convertTrinoTypeToFortressType(column.getType());

            Attribute attribute = Attribute.builder()
                    .name(column.getName())
                    .attributeType(trinoType)
                    .defaultValue(null)
                    .ordinalPosition(ordinalPosition++)
                    .isPrimary(false)
                    .isRequired(column.isNullable())
                    .isPartitionKey(false)
                    .description(column.getComment())
                    .metadata(null)
                    .build();
            attributes.add(attribute);
        }
        return attributes;
    }

    public static AttributeType convertTrinoTypeToFortressType(Type type)
    {
        log.info("Entering into DviewCreateTableUtils::convertTrinoTypeToFortressType");
        if (type instanceof BooleanType) {
            return AttributeType.builder().datatype(AttributeType.Type.BOOLEAN).defaultValue(false).build();
        }
        else if (type instanceof IntegerType) {
            return AttributeType.builder().datatype(AttributeType.Type.INT).defaultValue(0).build();
        }
        else if (type instanceof BigintType) {
            return AttributeType.builder().datatype(AttributeType.Type.BIGINT).defaultValue(0L).build();
        }
        else if (type instanceof FloatType) {
            return AttributeType.builder().datatype(AttributeType.Type.FLOAT).defaultValue(0.0f).build();
        }
        else if (type instanceof DoubleType) {
            return AttributeType.builder().datatype(AttributeType.Type.DOUBLE).defaultValue(0.0).build();
        }
        else if (type instanceof DateType) {
            return AttributeType.builder().datatype(AttributeType.Type.DATE).defaultValue(null).build();
        }
        else if (type instanceof TimestampType) {
            return AttributeType.builder().datatype(AttributeType.Type.TIMESTAMP).defaultValue(null).build();
        }
        else if (type instanceof TimeType) {
            return AttributeType.builder().datatype(AttributeType.Type.TIME).defaultValue(null).build();
        }
        else if (type instanceof CharType) {
            return AttributeType.builder().datatype(AttributeType.Type.CHAR).defaultValue("").build();
        }
        else if (type instanceof VarbinaryType) {
            return AttributeType.builder().datatype(AttributeType.Type.BINARY).defaultValue(null).build();
        }
        else if (type instanceof VarcharType) {
            return AttributeType.builder().datatype(AttributeType.Type.VARCHAR).defaultValue("").build();
        }
        else {
            throw new IllegalArgumentException("Unsupported Trino type: " + type.getClass().getName());
        }
    }
}
