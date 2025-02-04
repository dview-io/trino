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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class SftpColumnHandle
        implements ColumnHandle
{
    private final String name;
    private final Type type;
    private final int ordinalPosition;

    @JsonCreator
    public SftpColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("ordinalPosition") int ordinalPosition)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.ordinalPosition = ordinalPosition;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, ordinalPosition);
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
        SftpColumnHandle other = (SftpColumnHandle) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                this.ordinalPosition == other.ordinalPosition;
    }

    @Override
    public String toString()
    {
        return String.format("%s:%s:%d", name, type.getDisplayName(), ordinalPosition);
    }

    public ColumnMetadata toColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setHidden(false)
                .build();
    }
}
