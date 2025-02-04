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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class SftpTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;
    private final String directoryPath;
    private final String compressionType;
    private final String delimiter;
    private final boolean skipHeader;

    @JsonCreator
    public SftpTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("directoryPath") String directoryPath,
            @JsonProperty("compressionType") String compressionType,
            @JsonProperty("delimiter") String delimiter,
            @JsonProperty("skipHeader") boolean skipHeader)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.directoryPath = requireNonNull(directoryPath, "directoryPath is null");
        this.compressionType = compressionType; // can be null
        this.delimiter = delimiter != null ? delimiter : ",";
        this.skipHeader = skipHeader;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public String getDirectoryPath()
    {
        return directoryPath;
    }

    @JsonProperty
    public String getCompressionType()
    {
        return compressionType;
    }

    @JsonProperty
    public String getDelimiter()
    {
        return delimiter;
    }

    @JsonProperty
    public boolean isSkipHeader()
    {
        return skipHeader;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, directoryPath, compressionType, delimiter, skipHeader);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SftpTableHandle other = (SftpTableHandle) obj;
        return Objects.equals(this.schemaTableName, other.schemaTableName) &&
                Objects.equals(this.directoryPath, other.directoryPath) &&
                Objects.equals(this.compressionType, other.compressionType) &&
                Objects.equals(this.delimiter, other.delimiter) &&
                this.skipHeader == other.skipHeader;
    }

    @Override
    public String toString()
    {
        return String.format("SFTP(%s)", schemaTableName);
    }
}
