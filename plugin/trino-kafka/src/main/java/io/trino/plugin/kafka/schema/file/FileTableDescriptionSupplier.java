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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.decoder.dummy.DummyRowDecoder;
import io.trino.plugin.kafka.KafkaConfig;
import io.trino.plugin.kafka.KafkaTopicDescription;
import io.trino.plugin.kafka.KafkaTopicFieldGroup;
import io.trino.plugin.kafka.schema.TableDescriptionSupplier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.file.Files.readAllBytes;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class FileTableDescriptionSupplier
        implements TableDescriptionSupplier
{
    public static final String NAME = "file";

    private static final Logger log = Logger.get(FileTableDescriptionSupplier.class);

    private final JsonCodec<KafkaTopicDescription> topicDescriptionCodec;
    private final File tableDescriptionDir;
    private final String defaultSchema;
    private final Set<String> tableNames;
    private final LoadingCache<String, Map<SchemaTableName, KafkaTopicDescription>> tableDescriptionCache;

    @Inject
    public FileTableDescriptionSupplier(FileTableDescriptionSupplierConfig config, KafkaConfig kafkaConfig, JsonCodec<KafkaTopicDescription> topicDescriptionCodec)
    {
        this.topicDescriptionCodec = requireNonNull(topicDescriptionCodec, "topicDescriptionCodec is null");
        this.tableDescriptionDir = config.getTableDescriptionDir();
        this.defaultSchema = kafkaConfig.getDefaultSchema();
        this.tableNames = ImmutableSet.copyOf(config.getTableNames());

        this.tableDescriptionCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(config.getSchemaRefreshInterval().toMillis(), TimeUnit.MILLISECONDS)
                .build(CacheLoader.from(this::populateTables));
    }

    @Override
    public Set<SchemaTableName> listTables()
    {
        try {
            return ImmutableSet.copyOf(tableDescriptionCache.get(tableDescriptionDir.getAbsolutePath()).keySet());
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Failed to load table descriptions", e);
        }
    }

    @Override
    public Optional<KafkaTopicDescription> getTopicDescription(ConnectorSession session, SchemaTableName schemaTableName)
    {
        try {
            return Optional.ofNullable(tableDescriptionCache.get(tableDescriptionDir.getAbsolutePath()).get(schemaTableName));
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Failed to load table descriptions", e);
        }
    }

    private Map<SchemaTableName, KafkaTopicDescription> populateTables()
    {
        ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> builder = ImmutableMap.builder();

        log.debug("Loading kafka table definitions from %s", tableDescriptionDir.getAbsolutePath());

        try {
            for (File file : listFiles(tableDescriptionDir)) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    KafkaTopicDescription table = topicDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                    String schemaName = table.getSchemaName().orElse(defaultSchema);
                    log.debug("Kafka table %s.%s: %s", schemaName, table.getTableName(), table);
                    builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
                }
            }

            Map<SchemaTableName, KafkaTopicDescription> tableDefinitions = builder.buildOrThrow();

            log.debug("Loaded Table definitions: %s", tableDefinitions.keySet());

            builder = ImmutableMap.builder();
            for (String definedTable : tableNames) {
                SchemaTableName tableName;
                try {
                    tableName = parseTableName(definedTable);
                }
                catch (IllegalArgumentException iae) {
                    tableName = new SchemaTableName(defaultSchema, definedTable);
                }

                if (!tableDefinitions.containsKey(tableName)) {
                    log.debug("Table %s.%s does not have a table definition, using dummy decoder", tableName.getSchemaName(), tableName.getTableName());
                    builder.put(tableName, new KafkaTopicDescription(
                            tableName.getTableName(),
                            Optional.of(tableName.getSchemaName()),
                            definedTable,
                            Optional.of(new KafkaTopicFieldGroup(DummyRowDecoder.NAME, Optional.empty(), Optional.empty(), ImmutableList.of())),
                            Optional.of(new KafkaTopicFieldGroup(DummyRowDecoder.NAME, Optional.empty(), Optional.empty(), ImmutableList.of()))));
                }
                else {
                    builder.put(tableName, tableDefinitions.get(tableName));
                }
            }
            return builder.buildOrThrow();
        }
        catch (IOException e) {
            log.warn(e, "Error loading table definitions from %s", tableDescriptionDir.getAbsolutePath());
            throw new UncheckedIOException(e);
        }
    }

    private static List<File> listFiles(File dir)
    {
        if (dir != null && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                return asList(files);
            }
        }
        return ImmutableList.of();
    }

    private static SchemaTableName parseTableName(String schemaTableName)
    {
        checkArgument(!isNullOrEmpty(schemaTableName), "schemaTableName is null or empty");
        List<String> parts = Splitter.on('.').splitToList(schemaTableName);
        checkArgument(parts.size() == 2, "Invalid schemaTableName: %s", schemaTableName);
        return new SchemaTableName(parts.get(0), parts.get(1));
    }
}
