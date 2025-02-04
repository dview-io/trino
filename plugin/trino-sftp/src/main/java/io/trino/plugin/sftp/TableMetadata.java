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
package io.trino.plugin.sftp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TableMetadata
{
    private final SchemaTableName schemaTableName;
    private final List<ColumnMetadata> columns;
    private final String directoryPath;
    private final String compressionType;
    private final String delimiter;
    private final boolean skipHeader;
    private final Map<String, Object> additionalProperties;

    public TableMetadata(
            SchemaTableName schemaTableName,
            List<ColumnMetadata> columns,
            String directoryPath,
            String compressionType,
            String delimiter,
            boolean skipHeader,
            Map<String, Object> additionalProperties)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.directoryPath = requireNonNull(directoryPath, "directoryPath is null");
        this.compressionType = compressionType; // can be null
        this.delimiter = delimiter != null ? delimiter : ",";
        this.skipHeader = skipHeader;
        this.additionalProperties = ImmutableMap.copyOf(
                requireNonNull(additionalProperties, "additionalProperties is null"));
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }

    public String getDirectoryPath()
    {
        return directoryPath;
    }

    public String getCompressionType()
    {
        return compressionType;
    }

    public String getDelimiter()
    {
        return delimiter;
    }

    public boolean isSkipHeader()
    {
        return skipHeader;
    }

    public Map<String, Object> getAdditionalProperties()
    {
        return additionalProperties;
    }

    public ConnectorTableMetadata toConnectorTableMetadata()
    {
        // Combine all properties
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put("directory_path", directoryPath);
        if (compressionType != null) {
            properties.put("compression_type", compressionType);
        }
        properties.put("delimiter", delimiter);
        properties.put("skip_header", skipHeader);
        additionalProperties.forEach(properties::put);

        return new ConnectorTableMetadata(
                schemaTableName,
                columns,
                properties.buildOrThrow());
    }

    public SftpTableHandle toTableHandle()
    {
        return new SftpTableHandle(
                schemaTableName,
                directoryPath,
                compressionType,
                delimiter,
                skipHeader);
    }

    // Builder pattern for easier construction
    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private SchemaTableName schemaTableName;
        private List<ColumnMetadata> columns;
        private String directoryPath;
        private String compressionType;
        private String delimiter = ",";
        private boolean skipHeader;
        private Map<String, Object> additionalProperties = new HashMap<>();

        public Builder setSchemaTableName(SchemaTableName schemaTableName)
        {
            this.schemaTableName = schemaTableName;
            return this;
        }

        public Builder setColumns(List<ColumnMetadata> columns)
        {
            this.columns = columns;
            return this;
        }

        public Builder setDirectoryPath(String directoryPath)
        {
            this.directoryPath = directoryPath;
            return this;
        }

        public Builder setCompressionType(String compressionType)
        {
            this.compressionType = compressionType;
            return this;
        }

        public Builder setDelimiter(String delimiter)
        {
            this.delimiter = delimiter;
            return this;
        }

        public Builder setSkipHeader(boolean skipHeader)
        {
            this.skipHeader = skipHeader;
            return this;
        }

        public Builder setAdditionalProperties(Map<String, Object> additionalProperties)
        {
            this.additionalProperties = additionalProperties;
            return this;
        }

        public TableMetadata build()
        {
            return new TableMetadata(
                    schemaTableName,
                    columns,
                    directoryPath,
                    compressionType,
                    delimiter,
                    skipHeader,
                    additionalProperties);
        }
    }
}
