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
package io.trino.cli;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFormatUtils
{
    @Test
    public void testFormatCount()
    {
        assertThat(FormatUtils.formatCount(1L)).isEqualTo("1");
        assertThat(FormatUtils.formatCount(12L)).isEqualTo("12");
        assertThat(FormatUtils.formatCount(123L)).isEqualTo("123");
        assertThat(FormatUtils.formatCount(1234L)).isEqualTo("1.23K");
        assertThat(FormatUtils.formatCount(12345L)).isEqualTo("12.3K");
        assertThat(FormatUtils.formatCount(123456L)).isEqualTo("123K");
        assertThat(FormatUtils.formatCount(1234567L)).isEqualTo("1.23M");
        assertThat(FormatUtils.formatCount(12345678L)).isEqualTo("12.3M");
        assertThat(FormatUtils.formatCount(123456789L)).isEqualTo("123M");
        assertThat(FormatUtils.formatCount(1234567890L)).isEqualTo("1.23B");
        assertThat(FormatUtils.formatCount(12345678901L)).isEqualTo("12.3B");
        assertThat(FormatUtils.formatCount(123456789012L)).isEqualTo("123B");
        assertThat(FormatUtils.formatCount(1234567890123L)).isEqualTo("1.23T");
        assertThat(FormatUtils.formatCount(12345678901234L)).isEqualTo("12.3T");
        assertThat(FormatUtils.formatCount(123456789012345L)).isEqualTo("123T");
        assertThat(FormatUtils.formatCount(1234567890123456L)).isEqualTo("1.23Q");
        assertThat(FormatUtils.formatCount(12345678901234567L)).isEqualTo("12.3Q");
        assertThat(FormatUtils.formatCount(123456789012345678L)).isEqualTo("123Q");
        assertThat(FormatUtils.formatCount(1234567890123456789L)).isEqualTo("1235Q");
    }

    @Test
    public void formatCountRate()
    {
        assertThat(FormatUtils.formatCountRate(0.0000000001D, Duration.valueOf("1ns"), false))
                .isEqualTo("0");
        assertThat(FormatUtils.formatCountRate(0.0000000001D, Duration.valueOf("1ns"), true))
                .isEqualTo("0/s");
        assertThat(FormatUtils.formatCountRate(0.000000001D, Duration.valueOf("1ns"), false))
                .isEqualTo("1");
        assertThat(FormatUtils.formatCountRate(0.000000001D, Duration.valueOf("1ns"), true))
                .isEqualTo("1/s");
        assertThat(FormatUtils.formatCountRate(0.0000000015D, Duration.valueOf("1ns"), false))
                .isEqualTo("1");
        assertThat(FormatUtils.formatCountRate(0.0000000015D, Duration.valueOf("1ns"), true))
                .isEqualTo("1/s");
        assertThat(FormatUtils.formatCountRate(1D, Duration.valueOf("1ns"), false)).isEqualTo("1000M");
        assertThat(FormatUtils.formatCountRate(1D, Duration.valueOf("1ns"), true)).isEqualTo("1000M/s");
        assertThat(FormatUtils.formatCountRate(10.0D, Duration.valueOf("1ns"), false)).isEqualTo("10B");
        assertThat(FormatUtils.formatCountRate(10.0D, Duration.valueOf("1ns"), true)).isEqualTo("10B/s");
        assertThat(FormatUtils.formatCountRate(10.0D, Duration.valueOf("10ns"), false)).isEqualTo("1000M");
        assertThat(FormatUtils.formatCountRate(10.0D, Duration.valueOf("10ns"), true)).isEqualTo("1000M/s");

        assertThat(FormatUtils.formatCountRate(0.0000001D, Duration.valueOf("1us"), false)).isEqualTo("0");
        assertThat(FormatUtils.formatCountRate(0.0000001D, Duration.valueOf("1us"), true)).isEqualTo("0/s");
        assertThat(FormatUtils.formatCountRate(0.000001D, Duration.valueOf("1us"), false)).isEqualTo("1");
        assertThat(FormatUtils.formatCountRate(0.000001D, Duration.valueOf("1us"), true)).isEqualTo("1/s");
        assertThat(FormatUtils.formatCountRate(0.0000015D, Duration.valueOf("1us"), false)).isEqualTo("1");
        assertThat(FormatUtils.formatCountRate(0.0000015D, Duration.valueOf("1us"), true)).isEqualTo("1/s");
        assertThat(FormatUtils.formatCountRate(1D, Duration.valueOf("1us"), false)).isEqualTo("1000K");
        assertThat(FormatUtils.formatCountRate(1D, Duration.valueOf("1us"), true)).isEqualTo("1000K/s");
        assertThat(FormatUtils.formatCountRate(10.0D, Duration.valueOf("1us"), false)).isEqualTo("10M");
        assertThat(FormatUtils.formatCountRate(10.0D, Duration.valueOf("1us"), true)).isEqualTo("10M/s");
        assertThat(FormatUtils.formatCountRate(10.0D, Duration.valueOf("10us"), false)).isEqualTo("1000K");
        assertThat(FormatUtils.formatCountRate(10.0D, Duration.valueOf("10us"), true)).isEqualTo("1000K/s");

        assertThat(FormatUtils.formatCountRate(0.0001D, Duration.valueOf("1ms"), false)).isEqualTo("0");
        assertThat(FormatUtils.formatCountRate(0.0001D, Duration.valueOf("1ms"), true)).isEqualTo("0/s");
        assertThat(FormatUtils.formatCountRate(0.001D, Duration.valueOf("1ms"), false)).isEqualTo("1");
        assertThat(FormatUtils.formatCountRate(0.001D, Duration.valueOf("1ms"), true)).isEqualTo("1/s");
        assertThat(FormatUtils.formatCountRate(0.0015D, Duration.valueOf("1ms"), false)).isEqualTo("1");
        assertThat(FormatUtils.formatCountRate(0.0015D, Duration.valueOf("1ms"), true)).isEqualTo("1/s");
        assertThat(FormatUtils.formatCountRate(1D, Duration.valueOf("1ms"), false)).isEqualTo("1000");
        assertThat(FormatUtils.formatCountRate(1D, Duration.valueOf("1ms"), true)).isEqualTo("1000/s");
        assertThat(FormatUtils.formatCountRate(10.0D, Duration.valueOf("1ms"), false)).isEqualTo("10K");
        assertThat(FormatUtils.formatCountRate(10.0D, Duration.valueOf("1ms"), true)).isEqualTo("10K/s");
        assertThat(FormatUtils.formatCountRate(10.0D, Duration.valueOf("10ms"), false)).isEqualTo("1000");
        assertThat(FormatUtils.formatCountRate(10.0D, Duration.valueOf("10ms"), true)).isEqualTo("1000/s");

        assertThat(FormatUtils.formatCountRate(0.1D, Duration.valueOf("1s"), false)).isEqualTo("0");
        assertThat(FormatUtils.formatCountRate(0.1D, Duration.valueOf("1s"), true)).isEqualTo("0/s");
        assertThat(FormatUtils.formatCountRate(1.0D, Duration.valueOf("1s"), false)).isEqualTo("1");
        assertThat(FormatUtils.formatCountRate(1.0D, Duration.valueOf("1s"), true)).isEqualTo("1/s");
        assertThat(FormatUtils.formatCountRate(1.0D, Duration.valueOf("10s"), false)).isEqualTo("0");
        assertThat(FormatUtils.formatCountRate(1.0D, Duration.valueOf("10s"), true)).isEqualTo("0/s");
        assertThat(FormatUtils.formatCountRate(1.5D, Duration.valueOf("1s"), false)).isEqualTo("1");
        assertThat(FormatUtils.formatCountRate(1.5D, Duration.valueOf("1s"), true)).isEqualTo("1/s");
        assertThat(FormatUtils.formatCountRate(10.0D, Duration.valueOf("1s"), false)).isEqualTo("10");
        assertThat(FormatUtils.formatCountRate(10.0D, Duration.valueOf("1s"), true)).isEqualTo("10/s");
        assertThat(FormatUtils.formatCountRate(10.0D, Duration.valueOf("10s"), false)).isEqualTo("1");
        assertThat(FormatUtils.formatCountRate(10.0D, Duration.valueOf("10s"), true)).isEqualTo("1/s");

        assertThat(FormatUtils.formatCountRate(1.0D, Duration.valueOf("1m"), false)).isEqualTo("0");
        assertThat(FormatUtils.formatCountRate(1.0D, Duration.valueOf("1m"), true)).isEqualTo("0/s");
        assertThat(FormatUtils.formatCountRate(60.0D, Duration.valueOf("1m"), false)).isEqualTo("1");
        assertThat(FormatUtils.formatCountRate(60.0D, Duration.valueOf("1m"), true)).isEqualTo("1/s");
        assertThat(FormatUtils.formatCountRate(600.0D, Duration.valueOf("1m"), false)).isEqualTo("10");
        assertThat(FormatUtils.formatCountRate(600.0D, Duration.valueOf("1m"), true)).isEqualTo("10/s");
        assertThat(FormatUtils.formatCountRate(100.0D, Duration.valueOf("10m"), false)).isEqualTo("0");
        assertThat(FormatUtils.formatCountRate(100.0D, Duration.valueOf("10m"), true)).isEqualTo("0/s");
        assertThat(FormatUtils.formatCountRate(600.0D, Duration.valueOf("10m"), false)).isEqualTo("1");
        assertThat(FormatUtils.formatCountRate(600.0D, Duration.valueOf("10m"), true)).isEqualTo("1/s");
        assertThat(FormatUtils.formatCountRate(6000.0D, Duration.valueOf("10m"), false)).isEqualTo("10");
        assertThat(FormatUtils.formatCountRate(6000.0D, Duration.valueOf("10m"), true)).isEqualTo("10/s");

        assertThat(FormatUtils.formatCountRate(1.0D, Duration.valueOf("1h"), false)).isEqualTo("0");
        assertThat(FormatUtils.formatCountRate(1.0D, Duration.valueOf("1h"), true)).isEqualTo("0/s");
        assertThat(FormatUtils.formatCountRate(3600.0D, Duration.valueOf("1h"), false)).isEqualTo("1");
        assertThat(FormatUtils.formatCountRate(3600.0D, Duration.valueOf("1h"), true)).isEqualTo("1/s");
        assertThat(FormatUtils.formatCountRate(36000.0D, Duration.valueOf("1h"), false)).isEqualTo("10");
        assertThat(FormatUtils.formatCountRate(36000.0D, Duration.valueOf("1h"), true)).isEqualTo("10/s");
        assertThat(FormatUtils.formatCountRate(100.0D, Duration.valueOf("10h"), false)).isEqualTo("0");
        assertThat(FormatUtils.formatCountRate(100.0D, Duration.valueOf("10h"), true)).isEqualTo("0/s");
        assertThat(FormatUtils.formatCountRate(36000.0D, Duration.valueOf("10h"), false)).isEqualTo("1");
        assertThat(FormatUtils.formatCountRate(36000.0D, Duration.valueOf("10h"), true)).isEqualTo("1/s");
        assertThat(FormatUtils.formatCountRate(360000.0D, Duration.valueOf("10h"), false)).isEqualTo("10");
        assertThat(FormatUtils.formatCountRate(360000.0D, Duration.valueOf("10h"), true)).isEqualTo("10/s");

        assertThat(FormatUtils.formatCountRate(1.0D, Duration.valueOf("1d"), false)).isEqualTo("0");
        assertThat(FormatUtils.formatCountRate(1.0D, Duration.valueOf("1d"), true)).isEqualTo("0/s");
        assertThat(FormatUtils.formatCountRate(86400.0D, Duration.valueOf("1d"), false)).isEqualTo("1");
        assertThat(FormatUtils.formatCountRate(86400.0D, Duration.valueOf("1d"), true)).isEqualTo("1/s");
        assertThat(FormatUtils.formatCountRate(864000.0D, Duration.valueOf("1d"), false)).isEqualTo("10");
        assertThat(FormatUtils.formatCountRate(864000.0D, Duration.valueOf("1d"), true)).isEqualTo("10/s");
        assertThat(FormatUtils.formatCountRate(86400.0D, Duration.valueOf("10d"), false)).isEqualTo("0");
        assertThat(FormatUtils.formatCountRate(86400.0D, Duration.valueOf("10d"), true)).isEqualTo("0/s");
        assertThat(FormatUtils.formatCountRate(864000.0D, Duration.valueOf("10d"), false)).isEqualTo("1");
        assertThat(FormatUtils.formatCountRate(864000.0D, Duration.valueOf("10d"), true)).isEqualTo("1/s");
        assertThat(FormatUtils.formatCountRate(8640000.0D, Duration.valueOf("10d"), false)).isEqualTo("10");
        assertThat(FormatUtils.formatCountRate(8640000.0D, Duration.valueOf("10d"), true)).isEqualTo("10/s");

        assertThat(FormatUtils.formatCountRate(Double.NaN, Duration.valueOf("1s"), false)).isEqualTo("0");
        assertThat(FormatUtils.formatCountRate(Double.NaN, Duration.valueOf("1s"), true)).isEqualTo("0/s");
        assertThat(FormatUtils.formatCountRate(Double.POSITIVE_INFINITY, Duration.valueOf("1s"), false)).isEqualTo("0");
        assertThat(FormatUtils.formatCountRate(Double.POSITIVE_INFINITY, Duration.valueOf("1s"), true)).isEqualTo("0/s");
        assertThat(FormatUtils.formatCountRate(Double.NEGATIVE_INFINITY, Duration.valueOf("1s"), false)).isEqualTo("0");
        assertThat(FormatUtils.formatCountRate(Double.NEGATIVE_INFINITY, Duration.valueOf("1s"), true)).isEqualTo("0/s");
        assertThat(FormatUtils.formatCountRate(1.0D, Duration.valueOf("0s"), false)).isEqualTo("0");
        assertThat(FormatUtils.formatCountRate(1.0D, Duration.valueOf("0s"), true)).isEqualTo("0/s");
        assertThat(FormatUtils.formatCountRate(-1.0D, Duration.valueOf("0s"), false)).isEqualTo("0");
        assertThat(FormatUtils.formatCountRate(-1.0D, Duration.valueOf("0s"), true)).isEqualTo("0/s");
    }

    @Test
    public void testFormatDataSizeBinary()
    {
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1L), false, false)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1L), true, false)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12L), false, false)).isEqualTo("12B");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12L), true, false)).isEqualTo("12B");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123L), false, false)).isEqualTo("123B");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123L), true, false)).isEqualTo("123B");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234L), false, false)).isEqualTo("1.21Ki");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234L), true, false)).isEqualTo("1.21KiB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345L), false, false)).isEqualTo("12.1Ki");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345L), true, false)).isEqualTo("12.1KiB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456L), false, false)).isEqualTo("121Ki");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456L), true, false)).isEqualTo("121KiB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567L), false, false)).isEqualTo("1.18Mi");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567L), true, false)).isEqualTo("1.18MiB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345678L), false, false)).isEqualTo("11.8Mi");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345678L), true, false)).isEqualTo("11.8MiB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456789L), false, false)).isEqualTo("118Mi");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456789L), true, false)).isEqualTo("118MiB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890L), false, false)).isEqualTo("1.15Gi");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890L), true, false)).isEqualTo("1.15GiB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901L), false, false)).isEqualTo("11.5Gi");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901L), true, false)).isEqualTo("11.5GiB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012L), false, false)).isEqualTo("115Gi");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012L), true, false)).isEqualTo("115GiB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123L), false, false)).isEqualTo("1.12Ti");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123L), true, false)).isEqualTo("1.12TiB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901234L), false, false)).isEqualTo("11.2Ti");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901234L), true, false)).isEqualTo("11.2TiB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012345L), false, false)).isEqualTo("112Ti");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012345L), true, false)).isEqualTo("112TiB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123456L), false, false)).isEqualTo("1.1Pi");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123456L), true, false)).isEqualTo("1.1PiB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901234567L), false, false)).isEqualTo("11Pi");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901234567L), true, false)).isEqualTo("11PiB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012345678L), false, false)).isEqualTo("110Pi");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012345678L), true, false)).isEqualTo("110PiB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123456789L), false, false)).isEqualTo("1097Pi");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123456789L), true, false)).isEqualTo("1097PiB");
    }

    @Test
    public void testFormatDataSizeDecimal()
    {
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1L), false, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1L), true, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12L), false, true)).isEqualTo("12B");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12L), true, true)).isEqualTo("12B");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123L), false, true)).isEqualTo("123B");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123L), true, true)).isEqualTo("123B");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234L), false, true)).isEqualTo("1.23K");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234L), true, true)).isEqualTo("1.23KB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345L), false, true)).isEqualTo("12.3K");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345L), true, true)).isEqualTo("12.3KB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456L), false, true)).isEqualTo("123K");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456L), true, true)).isEqualTo("123KB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567L), false, true)).isEqualTo("1.23M");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567L), true, true)).isEqualTo("1.23MB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345678L), false, true)).isEqualTo("12.3M");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345678L), true, true)).isEqualTo("12.3MB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456789L), false, true)).isEqualTo("123M");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456789L), true, true)).isEqualTo("123MB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890L), false, true)).isEqualTo("1.23G");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890L), true, true)).isEqualTo("1.23GB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901L), false, true)).isEqualTo("12.3G");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901L), true, true)).isEqualTo("12.3GB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012L), false, true)).isEqualTo("123G");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012L), true, true)).isEqualTo("123GB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123L), false, true)).isEqualTo("1.23T");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123L), true, true)).isEqualTo("1.23TB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901234L), false, true)).isEqualTo("12.3T");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901234L), true, true)).isEqualTo("12.3TB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012345L), false, true)).isEqualTo("123T");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012345L), true, true)).isEqualTo("123TB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123456L), false, true)).isEqualTo("1.23P");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123456L), true, true)).isEqualTo("1.23PB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901234567L), false, true)).isEqualTo("12.3P");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(12345678901234567L), true, true)).isEqualTo("12.3PB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012345678L), false, true)).isEqualTo("123P");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(123456789012345678L), true, true)).isEqualTo("123PB");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123456789L), false, true)).isEqualTo("1235P");
        assertThat(FormatUtils.formatDataSize(DataSize.ofBytes(1234567890123456789L), true, true)).isEqualTo("1235PB");
    }

    @Test
    public void testFormatDataRateBinary()
    {
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1ns"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1ns"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1ns"), false, true)).isEqualTo("1G");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1ns"), true, true)).isEqualTo("1GB/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1ns"), false, true)).isEqualTo("10G");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1ns"), true, true)).isEqualTo("10GB/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10ns"), false, true)).isEqualTo("1G");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10ns"), true, true)).isEqualTo("1GB/s");

        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1us"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1us"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1us"), false, true)).isEqualTo("1M");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1us"), true, true)).isEqualTo("1MB/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1us"), false, true)).isEqualTo("10M");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1us"), true, true)).isEqualTo("10MB/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10us"), false, true)).isEqualTo("1M");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10us"), true, true)).isEqualTo("1MB/s");

        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1ms"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1ms"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1ms"), false, true)).isEqualTo("1K");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1ms"), true, true)).isEqualTo("1KB/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1ms"), false, true)).isEqualTo("10K");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1ms"), true, true)).isEqualTo("10KB/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10ms"), false, true)).isEqualTo("1K");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10ms"), true, true)).isEqualTo("1KB/s");

        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1s"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1s"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1s"), false, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1s"), true, true)).isEqualTo("1B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("10s"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("10s"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1s"), false, true)).isEqualTo("10B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1s"), true, true)).isEqualTo("10B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10s"), false, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10s"), true, true)).isEqualTo("1B/s");

        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1m"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1m"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(60), Duration.valueOf("1m"), false, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(60), Duration.valueOf("1m"), true, true)).isEqualTo("1B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(600), Duration.valueOf("1m"), false, true)).isEqualTo("10B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(600), Duration.valueOf("1m"), true, true)).isEqualTo("10B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(100), Duration.valueOf("10m"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(100), Duration.valueOf("10m"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(600), Duration.valueOf("10m"), false, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(600), Duration.valueOf("10m"), true, true)).isEqualTo("1B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(6000), Duration.valueOf("10m"), false, true)).isEqualTo("10B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(6000), Duration.valueOf("10m"), true, true)).isEqualTo("10B/s");

        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1h"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1h"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(3600), Duration.valueOf("1h"), false, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(3600), Duration.valueOf("1h"), true, true)).isEqualTo("1B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(36000), Duration.valueOf("1h"), false, true)).isEqualTo("10B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(36000), Duration.valueOf("1h"), true, true)).isEqualTo("10B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(100), Duration.valueOf("10h"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(100), Duration.valueOf("10h"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(36000), Duration.valueOf("10h"), false, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(36000), Duration.valueOf("10h"), true, true)).isEqualTo("1B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(360000), Duration.valueOf("10h"), false, true)).isEqualTo("10B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(360000), Duration.valueOf("10h"), true, true)).isEqualTo("10B/s");

        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1d"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1d"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(86400), Duration.valueOf("1d"), false, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(86400), Duration.valueOf("1d"), true, true)).isEqualTo("1B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(864000), Duration.valueOf("1d"), false, true)).isEqualTo("10B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(864000), Duration.valueOf("1d"), true, true)).isEqualTo("10B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(86400), Duration.valueOf("10d"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(86400), Duration.valueOf("10d"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(864000), Duration.valueOf("10d"), false, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(864000), Duration.valueOf("10d"), true, true)).isEqualTo("1B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(8640000), Duration.valueOf("10d"), false, true)).isEqualTo("10B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(8640000), Duration.valueOf("10d"), true, true)).isEqualTo("10B/s");

        // Currently, these tests fail due to https://github.com/trinodb/trino/issues/13093
        // assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("0s"), false)).isEqualTo("0B");
        // assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("0s"), true)).isEqualTo("0B/s");
    }

    @Test
    public void testFormatDataRateDecimal()
    {
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1ns"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1ns"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1ns"), false, true)).isEqualTo("1G");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1ns"), true, true)).isEqualTo("1GB/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1ns"), false, true)).isEqualTo("10G");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1ns"), true, true)).isEqualTo("10GB/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10ns"), false, true)).isEqualTo("1G");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10ns"), true, true)).isEqualTo("1GB/s");

        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1us"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1us"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1us"), false, true)).isEqualTo("1M");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1us"), true, true)).isEqualTo("1MB/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1us"), false, true)).isEqualTo("10M");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1us"), true, true)).isEqualTo("10MB/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10us"), false, true)).isEqualTo("1M");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10us"), true, true)).isEqualTo("1MB/s");

        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1ms"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1ms"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1ms"), false, true)).isEqualTo("1K");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1ms"), true, true)).isEqualTo("1KB/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1ms"), false, true)).isEqualTo("10K");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1ms"), true, true)).isEqualTo("10KB/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10ms"), false, true)).isEqualTo("1K");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10ms"), true, true)).isEqualTo("1KB/s");

        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1s"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(0), Duration.valueOf("1s"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1s"), false, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1s"), true, true)).isEqualTo("1B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("10s"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("10s"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1s"), false, true)).isEqualTo("10B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("1s"), true, true)).isEqualTo("10B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10s"), false, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(10), Duration.valueOf("10s"), true, true)).isEqualTo("1B/s");

        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1m"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1m"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(60), Duration.valueOf("1m"), false, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(60), Duration.valueOf("1m"), true, true)).isEqualTo("1B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(600), Duration.valueOf("1m"), false, true)).isEqualTo("10B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(600), Duration.valueOf("1m"), true, true)).isEqualTo("10B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(100), Duration.valueOf("10m"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(100), Duration.valueOf("10m"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(600), Duration.valueOf("10m"), false, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(600), Duration.valueOf("10m"), true, true)).isEqualTo("1B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(6000), Duration.valueOf("10m"), false, true)).isEqualTo("10B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(6000), Duration.valueOf("10m"), true, true)).isEqualTo("10B/s");

        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1h"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1h"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(3600), Duration.valueOf("1h"), false, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(3600), Duration.valueOf("1h"), true, true)).isEqualTo("1B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(36000), Duration.valueOf("1h"), false, true)).isEqualTo("10B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(36000), Duration.valueOf("1h"), true, true)).isEqualTo("10B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(100), Duration.valueOf("10h"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(100), Duration.valueOf("10h"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(36000), Duration.valueOf("10h"), false, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(36000), Duration.valueOf("10h"), true, true)).isEqualTo("1B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(360000), Duration.valueOf("10h"), false, true)).isEqualTo("10B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(360000), Duration.valueOf("10h"), true, true)).isEqualTo("10B/s");

        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1d"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("1d"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(86400), Duration.valueOf("1d"), false, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(86400), Duration.valueOf("1d"), true, true)).isEqualTo("1B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(864000), Duration.valueOf("1d"), false, true)).isEqualTo("10B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(864000), Duration.valueOf("1d"), true, true)).isEqualTo("10B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(86400), Duration.valueOf("10d"), false, true)).isEqualTo("0B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(86400), Duration.valueOf("10d"), true, true)).isEqualTo("0B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(864000), Duration.valueOf("10d"), false, true)).isEqualTo("1B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(864000), Duration.valueOf("10d"), true, true)).isEqualTo("1B/s");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(8640000), Duration.valueOf("10d"), false, true)).isEqualTo("10B");
        assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(8640000), Duration.valueOf("10d"), true, true)).isEqualTo("10B/s");

        // Currently, these tests fail due to https://github.com/trinodb/trino/issues/13093
        // assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("0s"), false)).isEqualTo("0B");
        // assertThat(FormatUtils.formatDataRate(DataSize.ofBytes(1), Duration.valueOf("0s"), true)).isEqualTo("0B/s");
    }

    @Test
    public void testPluralize()
    {
        assertThat(FormatUtils.pluralize("foo", 0)).isEqualTo("foos");
        assertThat(FormatUtils.pluralize("foo", 1)).isEqualTo("foo");
        assertThat(FormatUtils.pluralize("foo", 2)).isEqualTo("foos");
    }

    @Test
    public void testFormatFinalTime()
    {
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("0us"))).isEqualTo("0.00");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("0ns"))).isEqualTo("0.00");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("0ms"))).isEqualTo("0.00");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("0s"))).isEqualTo("0.00");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("0m"))).isEqualTo("0.00");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("0h"))).isEqualTo("0.00");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("0d"))).isEqualTo("0.00");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("1us"))).isEqualTo("0.00");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("1ns"))).isEqualTo("0.00");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("1ms"))).isEqualTo("0.00");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("10ms"))).isEqualTo("0.01");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("100ms"))).isEqualTo("0.10");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("1s"))).isEqualTo("1.00");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("10s"))).isEqualTo("10.00");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("1m"))).isEqualTo("1:00");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("61s"))).isEqualTo("1:01");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("1h"))).isEqualTo("60:00");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("61m"))).isEqualTo("61:00");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("1d"))).isEqualTo("1440:00");
        assertThat(FormatUtils.formatFinalTime(Duration.valueOf("25h"))).isEqualTo("1500:00");
    }

    @Test
    public void testFormatIndeterminateProgressBar()
    {
        assertThat(FormatUtils.formatProgressBar(10, 0)).isEqualTo("<=>       ");
        assertThat(FormatUtils.formatProgressBar(10, 1)).isEqualTo(" <=>      ");
        assertThat(FormatUtils.formatProgressBar(10, 7)).isEqualTo("       <=>");
        assertThat(FormatUtils.formatProgressBar(10, 8)).isEqualTo("      <=> ");
        assertThat(FormatUtils.formatProgressBar(10, 13)).isEqualTo(" <=>      ");
        assertThat(FormatUtils.formatProgressBar(10, 14)).isEqualTo("<=>       ");
    }

    @Test
    public void testInvalidIndeterminateProgressBar()
    {
        assertThatThrownBy(() -> FormatUtils.formatProgressBar(10, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid count: -1");
    }

    @Test
    public void testFormatProgressBar()
    {
        assertThat(FormatUtils.formatProgressBar(10, 0, 0))
                .isEqualTo("          ");
        assertThat(FormatUtils.formatProgressBar(10, 10, 10)).isEqualTo("=>        ");
        assertThat(FormatUtils.formatProgressBar(10, 10, 20)).isEqualTo("=>>       ");
        assertThat(FormatUtils.formatProgressBar(10, 20, 10)).isEqualTo("==>       ");
        assertThat(FormatUtils.formatProgressBar(10, 20, 20)).isEqualTo("==>>      ");
        assertThat(FormatUtils.formatProgressBar(10, 50, 50)).isEqualTo("=====>>>>>");
        assertThat(FormatUtils.formatProgressBar(10, 100, 0)).isEqualTo("==========");
        assertThat(FormatUtils.formatProgressBar(10, 0, 100)).isEqualTo(">>>>>>>>>>");
        assertThat(FormatUtils.formatProgressBar(10, 60, 60)).isEqualTo("======>>>>");
        assertThat(FormatUtils.formatProgressBar(10, 120, 0)).isEqualTo("==========");
        assertThat(FormatUtils.formatProgressBar(10, 0, 120)).isEqualTo(">>>>>>>>>>");
    }

    @Test
    public void testInvalidProgressBar()
    {
        assertThatThrownBy(() -> FormatUtils.formatProgressBar(10, -100, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid count: -9");
    }
}
