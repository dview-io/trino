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
package io.trino.plugin.hive.aws.athena;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Domain;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_FORMAT;
import static io.trino.plugin.hive.aws.athena.PartitionProjectionProperties.COLUMN_PROJECTION_RANGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDateProjectionFactory
{
    @Test
    void testIsSupported()
    {
        assertThat(new DateProjectionFactory().isSupportedColumnType(VARCHAR)).isTrue();
        assertThat(new DateProjectionFactory().isSupportedColumnType(DATE)).isTrue();
        assertThat(new DateProjectionFactory().isSupportedColumnType(TIMESTAMP_SECONDS)).isTrue();
        assertThat(new DateProjectionFactory().isSupportedColumnType(TIMESTAMP_MICROS)).isTrue();
        assertThat(new DateProjectionFactory().isSupportedColumnType(TIMESTAMP_NANOS)).isFalse();
        assertThat(new DateProjectionFactory().isSupportedColumnType(BIGINT)).isFalse();
    }

    @Test
    void testCreate()
    {
        Projection projection = new DateProjectionFactory().create("test", VARCHAR, ImmutableMap.of(COLUMN_PROJECTION_FORMAT, "yyyy-MM-dd", COLUMN_PROJECTION_RANGE, ImmutableList.of("2020-01-01", "2020-01-03")));
        assertThat(projection.getProjectedValues(Optional.empty())).containsExactly("2020-01-01", "2020-01-02", "2020-01-03");
        assertThat(projection.getProjectedValues(Optional.of(Domain.all(VARCHAR)))).containsExactly("2020-01-01", "2020-01-02", "2020-01-03");
        assertThat(projection.getProjectedValues(Optional.of(Domain.none(VARCHAR)))).isEmpty();
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice("2020-01-02"))))).containsExactly("2020-01-02");
        assertThat(projection.getProjectedValues(Optional.of(Domain.singleValue(VARCHAR, Slices.utf8Slice("2222-01-01"))))).isEmpty();

        assertThatThrownBy(() -> new DateProjectionFactory().create("test", VARCHAR, ImmutableMap.of("ignored", ImmutableList.of("2020-01-01", "2020-01-02", "2020-01-03"))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Column projection for column 'test' failed. Missing required property: 'partition_projection_format'");
    }
}
