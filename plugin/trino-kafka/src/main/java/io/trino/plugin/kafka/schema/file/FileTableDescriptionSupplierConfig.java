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
package io.trino.plugin.kafka.schema.file;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.Set;

public class FileTableDescriptionSupplierConfig
{
    private Set<String> tableNames = ImmutableSet.of();
    private File tableDescriptionDir = new File("etc/kafka/");
    private long schemaRefreshInterval = 300000; // Default 300 seconds

    public Set<String> getTableNames()
    {
        return tableNames;
    }

    @Config("kafka.table-names")
    @ConfigDescription("Optional set of tables known to this connector. If not specified, table names will be derived from JSON schema files.")
    public FileTableDescriptionSupplierConfig setTableNames(String tableNames)
    {
        if (!isNullOrEmpty(tableNames)) {
            this.tableNames = ImmutableSet.copyOf(
                    Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
        }
        return this;
    }

    public void updateTableNames(Set<String> tableNames)
    {
        this.tableNames = ImmutableSet.copyOf(tableNames);
    }

    @NotNull
    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("kafka.table-description-dir")
    @ConfigDescription("Folder holding JSON description files for Kafka topics")
    public FileTableDescriptionSupplierConfig setTableDescriptionDir(File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    public long getSchemaRefreshInterval()
    {
        return schemaRefreshInterval;
    }

    @Config("kafka.schema-refresh-interval")
    @ConfigDescription("How frequently to refresh the schema from the table description files (in milliseconds)")
    public FileTableDescriptionSupplierConfig setSchemaRefreshInterval(long schemaRefreshInterval)
    {
        this.schemaRefreshInterval = schemaRefreshInterval;
        return this;
    }

    private boolean isNullOrEmpty(String str)
    {
        return str == null || str.isEmpty();
    }
}
