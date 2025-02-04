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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.dview.DviewConfig;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;

public class DviewTableProperties
{
    public static final String EXTERNAL_LOCATION_PROPERTY = "external_location";
    public static final String STORAGE_FORMAT_PROPERTY = "format";
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public DviewTableProperties(DviewConfig dviewConfig,
                                TypeManager typeManager)
    {
        tableProperties = ImmutableList.of(
                stringProperty(
                        EXTERNAL_LOCATION_PROPERTY,
                        "File system location URI for external table",
                        null,
                        false),
                stringProperty(
                        STORAGE_FORMAT_PROPERTY,
                        "Storage format for the table",
                        null,
                        false),
                new PropertyMetadata<>(
                        PARTITIONED_BY_PROPERTY,
                        "Partition columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((List<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static String getExternalLocation(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(EXTERNAL_LOCATION_PROPERTY);
    }

    public static String getStorageFormat(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(STORAGE_FORMAT_PROPERTY);
    }

    public static List<String> getPartitionedBy(Map<String, Object> tableProperties)
    {
        List<String> partitionedBy = (List<String>) tableProperties.get(PARTITIONED_BY_PROPERTY);
        return partitionedBy == null ? ImmutableList.of() : ImmutableList.copyOf(partitionedBy);
    }
}
