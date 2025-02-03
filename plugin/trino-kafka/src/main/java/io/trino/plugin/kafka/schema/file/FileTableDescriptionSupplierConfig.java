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
import io.airlift.units.Duration;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class FileTableDescriptionSupplierConfig
{
    private final AtomicReference<Set<String>> tableNames = new AtomicReference<>(ImmutableSet.of());
    private File tableDescriptionDir = new File("etc/kafka/");
    private Duration schemaRefreshInterval = new Duration(1, TimeUnit.MINUTES);

    @NotNull
    public Set<String> getTableNames()
    {
        return tableNames.get();
    }

    @Config("kafka.table-names")
    @ConfigDescription("Set of tables known to this connector")
    public FileTableDescriptionSupplierConfig setTableNames(String tableNames)
    {
        if (tableNames != null && !tableNames.isEmpty()) {
            this.tableNames.set(ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames)));
        }
        return this;
    }

    public void updateTableNames(Set<String> newTableNames)
    {
        this.tableNames.set(ImmutableSet.copyOf(newTableNames));
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

    @NotNull
    public Duration getSchemaRefreshInterval()
    {
        return schemaRefreshInterval;
    }

    @Config("kafka.schema-refresh-interval")
    @ConfigDescription("How frequently to refresh the schema from the JSON files")
    public FileTableDescriptionSupplierConfig setSchemaRefreshInterval(Duration schemaRefreshInterval)
    {
        this.schemaRefreshInterval = schemaRefreshInterval;
        return this;
    }
}
