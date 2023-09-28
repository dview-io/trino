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
package io.trino.plugin.dview.split;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;

public class DviewSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(DviewSplit.class);
    private final List<HostAddress> addresses;
    private final String partitionDate;
    private final String partitionTime;
    private final String filePath;

    @JsonCreator
    public DviewSplit(@JsonProperty("addresses") List<HostAddress> addresses, @JsonProperty("filePath") String filePath, @JsonProperty("partitionDate")String partitionDate, @JsonProperty("partitionTime")String partitionTime)
    {
        this.addresses = addresses;
        this.partitionDate = partitionDate;
        this.partitionTime = partitionTime;
        this.filePath = filePath;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return this.addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @JsonProperty
    public String getPartitionDate()
    {
        return partitionDate;
    }

    @JsonProperty
    public String getPartitionTime()
    {
        return partitionTime;
    }

    @JsonProperty
    public String getFilePath()
    {
        return filePath;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public String toString()
    {
        return "DviewSplit{" +
                "addresses=" + addresses +
                ", partitionDate='" + partitionDate + '\'' +
                ", partitionTime='" + partitionTime + '\'' +
                ", filePath='" + filePath + '\'' +
                '}';
    }
}
