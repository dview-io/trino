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
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record SftpSplit(String filePath, long fileSize, long start, long length, List<HostAddress> hostAddress)
        implements ConnectorSplit
{
    @JsonCreator
    public SftpSplit(
            @JsonProperty("filePath") String filePath,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("hostAddresses") List<HostAddress> hostAddress)
    {
        this.filePath = requireNonNull(filePath, "filePath is null");
        this.fileSize = fileSize;
        this.start = start;
        this.length = length;
        this.hostAddress = requireNonNull(hostAddress, "hostAddress is null");
    }

    @Override
    @JsonProperty
    public String filePath()
    {
        return filePath;
    }

    @Override
    @JsonProperty
    public long fileSize()
    {
        return fileSize;
    }

    @Override
    @JsonProperty
    public long start()
    {
        return start;
    }

    @Override
    @JsonProperty
    public long length()
    {
        return length;
    }

    @Override
    @JsonProperty
    public List<HostAddress> hostAddress()
    {
        return hostAddress;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return hostAddress;
    }

    @Override
    public String toString()
    {
        return String.format("SFTP Split[%s](size=%d, start=%d, length=%d)",
                filePath, fileSize, start, length);
    }
}
