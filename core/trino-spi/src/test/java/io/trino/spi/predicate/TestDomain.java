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
package io.trino.spi.predicate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.block.TestingBlockJsonSerde;
import io.trino.spi.type.TestingTypeDeserializer;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TestingIdType.ID;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDomain
{
    @Test
    public void testOrderableNone()
    {
        Domain domain = Domain.none(BIGINT);
        assertThat(domain.isNone()).isTrue();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isNullAllowed()).isFalse();
        assertThat(domain.getValues()).isEqualTo(ValueSet.none(BIGINT));
        assertThat(domain.getType()).isEqualTo(BIGINT);
        assertThat(domain.includesNullableValue(Long.MIN_VALUE)).isFalse();
        assertThat(domain.includesNullableValue(0L)).isFalse();
        assertThat(domain.includesNullableValue(Long.MAX_VALUE)).isFalse();
        assertThat(domain.includesNullableValue(null)).isFalse();
        assertThat(domain.complement()).isEqualTo(Domain.all(BIGINT));
        assertThat(domain.toString()).isEqualTo("NONE");
    }

    @Test
    public void testEquatableNone()
    {
        Domain domain = Domain.none(ID);
        assertThat(domain.isNone()).isTrue();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isNullAllowed()).isFalse();
        assertThat(domain.getValues()).isEqualTo(ValueSet.none(ID));
        assertThat(domain.getType()).isEqualTo(ID);
        assertThat(domain.includesNullableValue(0L)).isFalse();
        assertThat(domain.includesNullableValue(null)).isFalse();
        assertThat(domain.complement()).isEqualTo(Domain.all(ID));
        assertThat(domain.toString()).isEqualTo("NONE");
    }

    @Test
    public void testUncomparableNone()
    {
        Domain domain = Domain.none(HYPER_LOG_LOG);
        assertThat(domain.isNone()).isTrue();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isNullAllowed()).isFalse();
        assertThat(domain.getValues()).isEqualTo(ValueSet.none(HYPER_LOG_LOG));
        assertThat(domain.getType()).isEqualTo(HYPER_LOG_LOG);
        assertThat(domain.includesNullableValue(Slices.EMPTY_SLICE)).isFalse();
        assertThat(domain.includesNullableValue(null)).isFalse();
        assertThat(domain.complement()).isEqualTo(Domain.all(HYPER_LOG_LOG));
        assertThat(domain.toString()).isEqualTo("NONE");
    }

    @Test
    public void testOrderableAll()
    {
        Domain domain = Domain.all(BIGINT);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isTrue();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isTrue();
        assertThat(domain.getValues()).isEqualTo(ValueSet.all(BIGINT));
        assertThat(domain.getType()).isEqualTo(BIGINT);
        assertThat(domain.includesNullableValue(Long.MIN_VALUE)).isTrue();
        assertThat(domain.includesNullableValue(0L)).isTrue();
        assertThat(domain.includesNullableValue(Long.MAX_VALUE)).isTrue();
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.complement()).isEqualTo(Domain.none(BIGINT));
        assertThat(domain.toString()).isEqualTo("ALL");
    }

    @Test
    public void testFloatingPointOrderableAll()
    {
        Domain domain = Domain.all(REAL);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isTrue();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isTrue();
        assertThat(domain.getValues()).isEqualTo(ValueSet.all(REAL));
        assertThat(domain.getType()).isEqualTo(REAL);
        assertThat(domain.includesNullableValue((long) floatToRawIntBits(-Float.MAX_VALUE))).isTrue();
        assertThat(domain.includesNullableValue((long) floatToRawIntBits(0.0f))).isTrue();
        assertThat(domain.includesNullableValue((long) floatToRawIntBits(Float.MAX_VALUE))).isTrue();
        assertThat(domain.includesNullableValue((long) floatToRawIntBits(Float.MIN_VALUE))).isTrue();
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.includesNullableValue((long) floatToRawIntBits(Float.NaN))).isTrue();
        assertThat(domain.includesNullableValue((long) 0x7fc01234)).isTrue(); // different NaN representation
        assertThat(domain.complement()).isEqualTo(Domain.none(REAL));
        assertThat(domain.toString()).isEqualTo("ALL");

        domain = Domain.all(DOUBLE);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isTrue();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isTrue();
        assertThat(domain.getValues()).isEqualTo(ValueSet.all(DOUBLE));
        assertThat(domain.getType()).isEqualTo(DOUBLE);
        assertThat(domain.includesNullableValue(-Double.MAX_VALUE)).isTrue();
        assertThat(domain.includesNullableValue(0.0)).isTrue();
        assertThat(domain.includesNullableValue(Double.MAX_VALUE)).isTrue();
        assertThat(domain.includesNullableValue(Double.MIN_VALUE)).isTrue();
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.includesNullableValue(Double.NaN)).isTrue();
        assertThat(domain.includesNullableValue(longBitsToDouble(0x7ff8123412341234L))).isTrue(); // different NaN representation
        assertThat(domain.complement()).isEqualTo(Domain.none(DOUBLE));
        assertThat(domain.toString()).isEqualTo("ALL");
    }

    @Test
    public void testEquatableAll()
    {
        Domain domain = Domain.all(ID);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isTrue();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isTrue();
        assertThat(domain.getValues()).isEqualTo(ValueSet.all(ID));
        assertThat(domain.getType()).isEqualTo(ID);
        assertThat(domain.includesNullableValue(0L)).isTrue();
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.complement()).isEqualTo(Domain.none(ID));
        assertThat(domain.toString()).isEqualTo("ALL");
    }

    @Test
    public void testUncomparableAll()
    {
        Domain domain = Domain.all(HYPER_LOG_LOG);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isTrue();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isTrue();
        assertThat(domain.getValues()).isEqualTo(ValueSet.all(HYPER_LOG_LOG));
        assertThat(domain.getType()).isEqualTo(HYPER_LOG_LOG);
        assertThat(domain.includesNullableValue(Slices.EMPTY_SLICE)).isTrue();
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.complement()).isEqualTo(Domain.none(HYPER_LOG_LOG));
        assertThat(domain.toString()).isEqualTo("ALL");
    }

    @Test
    public void testOrderableNullOnly()
    {
        Domain domain = Domain.onlyNull(BIGINT);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullAllowed()).isTrue();
        assertThat(domain.isNullableSingleValue()).isTrue();
        assertThat(domain.isOnlyNull()).isTrue();
        assertThat(domain.getValues()).isEqualTo(ValueSet.none(BIGINT));
        assertThat(domain.getType()).isEqualTo(BIGINT);
        assertThat(domain.includesNullableValue(Long.MIN_VALUE)).isFalse();
        assertThat(domain.includesNullableValue(0L)).isFalse();
        assertThat(domain.includesNullableValue(Long.MAX_VALUE)).isFalse();
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.complement()).isEqualTo(Domain.notNull(BIGINT));
        assertThat(domain.getNullableSingleValue()).isEqualTo(null);
        assertThat(domain.toString()).isEqualTo("[NULL]");
    }

    @Test
    public void testEquatableNullOnly()
    {
        Domain domain = Domain.onlyNull(ID);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isTrue();
        assertThat(domain.isOnlyNull()).isTrue();
        assertThat(domain.isNullAllowed()).isTrue();
        assertThat(domain.getValues()).isEqualTo(ValueSet.none(ID));
        assertThat(domain.getType()).isEqualTo(ID);
        assertThat(domain.includesNullableValue(0L)).isFalse();
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.complement()).isEqualTo(Domain.notNull(ID));
        assertThat(domain.getNullableSingleValue()).isEqualTo(null);
        assertThat(domain.toString()).isEqualTo("[NULL]");
    }

    @Test
    public void testUncomparableNullOnly()
    {
        Domain domain = Domain.onlyNull(HYPER_LOG_LOG);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isTrue();
        assertThat(domain.isOnlyNull()).isTrue();
        assertThat(domain.isNullAllowed()).isTrue();
        assertThat(domain.getValues()).isEqualTo(ValueSet.none(HYPER_LOG_LOG));
        assertThat(domain.getType()).isEqualTo(HYPER_LOG_LOG);
        assertThat(domain.includesNullableValue(Slices.EMPTY_SLICE)).isFalse();
        assertThat(domain.includesNullableValue(null)).isTrue();
        assertThat(domain.complement()).isEqualTo(Domain.notNull(HYPER_LOG_LOG));
        assertThat(domain.getNullableSingleValue()).isEqualTo(null);
        assertThat(domain.toString()).isEqualTo("[NULL]");
    }

    @Test
    public void testOrderableNotNull()
    {
        Domain domain = Domain.notNull(BIGINT);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isFalse();
        assertThat(domain.getValues()).isEqualTo(ValueSet.all(BIGINT));
        assertThat(domain.getType()).isEqualTo(BIGINT);
        assertThat(domain.includesNullableValue(Long.MIN_VALUE)).isTrue();
        assertThat(domain.includesNullableValue(0L)).isTrue();
        assertThat(domain.includesNullableValue(Long.MAX_VALUE)).isTrue();
        assertThat(domain.includesNullableValue(null)).isFalse();
        assertThat(domain.complement()).isEqualTo(Domain.onlyNull(BIGINT));
        assertThat(domain.toString()).isEqualTo("[ SortedRangeSet[type=bigint, ranges=1, {(<min>,<max>)}] ]");
    }

    @Test
    public void testEquatableNotNull()
    {
        Domain domain = Domain.notNull(ID);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isFalse();
        assertThat(domain.getValues()).isEqualTo(ValueSet.all(ID));
        assertThat(domain.getType()).isEqualTo(ID);
        assertThat(domain.includesNullableValue(0L)).isTrue();
        assertThat(domain.includesNullableValue(null)).isFalse();
        assertThat(domain.complement()).isEqualTo(Domain.onlyNull(ID));
        assertThat(domain.toString()).isEqualTo("[ EquatableValueSet[type=id, values=0, EXCLUDES{}] ]");
    }

    @Test
    public void testUncomparableNotNull()
    {
        Domain domain = Domain.notNull(HYPER_LOG_LOG);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isFalse();
        assertThat(domain.isNullableSingleValue()).isFalse();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isFalse();
        assertThat(domain.getValues()).isEqualTo(ValueSet.all(HYPER_LOG_LOG));
        assertThat(domain.getType()).isEqualTo(HYPER_LOG_LOG);
        assertThat(domain.includesNullableValue(Slices.EMPTY_SLICE)).isTrue();
        assertThat(domain.includesNullableValue(null)).isFalse();
        assertThat(domain.complement()).isEqualTo(Domain.onlyNull(HYPER_LOG_LOG));
        assertThat(domain.toString()).isEqualTo("[ [ALL] ]");
    }

    @Test
    public void testOrderableSingleValue()
    {
        Domain domain = Domain.singleValue(BIGINT, 0L);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isTrue();
        assertThat(domain.isNullableSingleValue()).isTrue();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isFalse();
        assertThat(domain.getValues()).isEqualTo(ValueSet.ofRanges(Range.equal(BIGINT, 0L)));
        assertThat(domain.getType()).isEqualTo(BIGINT);
        assertThat(domain.includesNullableValue(Long.MIN_VALUE)).isFalse();
        assertThat(domain.includesNullableValue(0L)).isTrue();
        assertThat(domain.includesNullableValue(Long.MAX_VALUE)).isFalse();
        assertThat(domain.complement()).isEqualTo(Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 0L)), true));
        assertThat(domain.getSingleValue()).isEqualTo(0L);
        assertThat(domain.getNullableSingleValue()).isEqualTo(0L);
        assertThat(domain.toString()).isEqualTo("[ SortedRangeSet[type=bigint, ranges=1, {[0]}] ]");

        assertThatThrownBy(() -> Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 2L, true)), false).getSingleValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Domain is not a single value");
    }

    @Test
    public void testEquatableSingleValue()
    {
        Domain domain = Domain.singleValue(ID, 0L);
        assertThat(domain.isNone()).isFalse();
        assertThat(domain.isAll()).isFalse();
        assertThat(domain.isSingleValue()).isTrue();
        assertThat(domain.isNullableSingleValue()).isTrue();
        assertThat(domain.isOnlyNull()).isFalse();
        assertThat(domain.isNullAllowed()).isFalse();
        assertThat(domain.getValues()).isEqualTo(ValueSet.of(ID, 0L));
        assertThat(domain.getType()).isEqualTo(ID);
        assertThat(domain.includesNullableValue(0L)).isTrue();
        assertThat(domain.includesNullableValue(null)).isFalse();
        assertThat(domain.complement()).isEqualTo(Domain.create(ValueSet.of(ID, 0L).complement(), true));
        assertThat(domain.getSingleValue()).isEqualTo(0L);
        assertThat(domain.getNullableSingleValue()).isEqualTo(0L);
        assertThat(domain.toString()).isEqualTo("[ EquatableValueSet[type=id, values=1, {0}] ]");

        assertThatThrownBy(() -> Domain.create(ValueSet.of(ID, 0L, 1L), false).getSingleValue())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Domain is not a single value");
    }

    @Test
    public void testUncomparableSingleValue()
    {
        assertThatThrownBy(() -> Domain.singleValue(HYPER_LOG_LOG, Slices.EMPTY_SLICE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot create discrete ValueSet with non-comparable type: HyperLogLog");
    }

    @Test
    public void testOverlaps()
    {
        assertThat(Domain.all(BIGINT).overlaps(Domain.all(BIGINT))).isTrue();
        assertThat(Domain.all(BIGINT).overlaps(Domain.none(BIGINT))).isFalse();
        assertThat(Domain.all(BIGINT).overlaps(Domain.notNull(BIGINT))).isTrue();
        assertThat(Domain.all(BIGINT).overlaps(Domain.onlyNull(BIGINT))).isTrue();
        assertThat(Domain.all(BIGINT).overlaps(Domain.singleValue(BIGINT, 0L))).isTrue();

        assertThat(Domain.none(BIGINT).overlaps(Domain.all(BIGINT))).isFalse();
        assertThat(Domain.none(BIGINT).overlaps(Domain.none(BIGINT))).isFalse();
        assertThat(Domain.none(BIGINT).overlaps(Domain.notNull(BIGINT))).isFalse();
        assertThat(Domain.none(BIGINT).overlaps(Domain.onlyNull(BIGINT))).isFalse();
        assertThat(Domain.none(BIGINT).overlaps(Domain.singleValue(BIGINT, 0L))).isFalse();

        assertThat(Domain.notNull(BIGINT).overlaps(Domain.all(BIGINT))).isTrue();
        assertThat(Domain.notNull(BIGINT).overlaps(Domain.none(BIGINT))).isFalse();
        assertThat(Domain.notNull(BIGINT).overlaps(Domain.notNull(BIGINT))).isTrue();
        assertThat(Domain.notNull(BIGINT).overlaps(Domain.onlyNull(BIGINT))).isFalse();
        assertThat(Domain.notNull(BIGINT).overlaps(Domain.singleValue(BIGINT, 0L))).isTrue();

        assertThat(Domain.onlyNull(BIGINT).overlaps(Domain.all(BIGINT))).isTrue();
        assertThat(Domain.onlyNull(BIGINT).overlaps(Domain.none(BIGINT))).isFalse();
        assertThat(Domain.onlyNull(BIGINT).overlaps(Domain.notNull(BIGINT))).isFalse();
        assertThat(Domain.onlyNull(BIGINT).overlaps(Domain.onlyNull(BIGINT))).isTrue();
        assertThat(Domain.onlyNull(BIGINT).overlaps(Domain.singleValue(BIGINT, 0L))).isFalse();

        assertThat(Domain.singleValue(BIGINT, 0L).overlaps(Domain.all(BIGINT))).isTrue();
        assertThat(Domain.singleValue(BIGINT, 0L).overlaps(Domain.none(BIGINT))).isFalse();
        assertThat(Domain.singleValue(BIGINT, 0L).overlaps(Domain.notNull(BIGINT))).isTrue();
        assertThat(Domain.singleValue(BIGINT, 0L).overlaps(Domain.onlyNull(BIGINT))).isFalse();
        assertThat(Domain.singleValue(BIGINT, 0L).overlaps(Domain.singleValue(BIGINT, 0L))).isTrue();
    }

    @Test
    public void testContains()
    {
        assertThat(Domain.all(BIGINT).contains(Domain.all(BIGINT))).isTrue();
        assertThat(Domain.all(BIGINT).contains(Domain.none(BIGINT))).isTrue();
        assertThat(Domain.all(BIGINT).contains(Domain.notNull(BIGINT))).isTrue();
        assertThat(Domain.all(BIGINT).contains(Domain.onlyNull(BIGINT))).isTrue();
        assertThat(Domain.all(BIGINT).contains(Domain.singleValue(BIGINT, 0L))).isTrue();

        assertThat(Domain.none(BIGINT).contains(Domain.all(BIGINT))).isFalse();
        assertThat(Domain.none(BIGINT).contains(Domain.none(BIGINT))).isTrue();
        assertThat(Domain.none(BIGINT).contains(Domain.notNull(BIGINT))).isFalse();
        assertThat(Domain.none(BIGINT).contains(Domain.onlyNull(BIGINT))).isFalse();
        assertThat(Domain.none(BIGINT).contains(Domain.singleValue(BIGINT, 0L))).isFalse();

        assertThat(Domain.notNull(BIGINT).contains(Domain.all(BIGINT))).isFalse();
        assertThat(Domain.notNull(BIGINT).contains(Domain.none(BIGINT))).isTrue();
        assertThat(Domain.notNull(BIGINT).contains(Domain.notNull(BIGINT))).isTrue();
        assertThat(Domain.notNull(BIGINT).contains(Domain.onlyNull(BIGINT))).isFalse();
        assertThat(Domain.notNull(BIGINT).contains(Domain.singleValue(BIGINT, 0L))).isTrue();

        assertThat(Domain.onlyNull(BIGINT).contains(Domain.all(BIGINT))).isFalse();
        assertThat(Domain.onlyNull(BIGINT).contains(Domain.none(BIGINT))).isTrue();
        assertThat(Domain.onlyNull(BIGINT).contains(Domain.notNull(BIGINT))).isFalse();
        assertThat(Domain.onlyNull(BIGINT).contains(Domain.onlyNull(BIGINT))).isTrue();
        assertThat(Domain.onlyNull(BIGINT).contains(Domain.singleValue(BIGINT, 0L))).isFalse();

        assertThat(Domain.singleValue(BIGINT, 0L).contains(Domain.all(BIGINT))).isFalse();
        assertThat(Domain.singleValue(BIGINT, 0L).contains(Domain.none(BIGINT))).isTrue();
        assertThat(Domain.singleValue(BIGINT, 0L).contains(Domain.notNull(BIGINT))).isFalse();
        assertThat(Domain.singleValue(BIGINT, 0L).contains(Domain.onlyNull(BIGINT))).isFalse();
        assertThat(Domain.singleValue(BIGINT, 0L).contains(Domain.singleValue(BIGINT, 0L))).isTrue();
    }

    @Test
    public void testIntersect()
    {
        assertThat(Domain.all(BIGINT).intersect(Domain.all(BIGINT))).isEqualTo(Domain.all(BIGINT));

        assertThat(Domain.none(BIGINT).intersect(Domain.none(BIGINT))).isEqualTo(Domain.none(BIGINT));

        assertThat(Domain.all(BIGINT).intersect(Domain.none(BIGINT))).isEqualTo(Domain.none(BIGINT));

        assertThat(Domain.notNull(BIGINT).intersect(Domain.onlyNull(BIGINT))).isEqualTo(Domain.none(BIGINT));

        assertThat(Domain.singleValue(BIGINT, 0L).intersect(Domain.all(BIGINT))).isEqualTo(Domain.singleValue(BIGINT, 0L));

        assertThat(Domain.singleValue(BIGINT, 0L).intersect(Domain.onlyNull(BIGINT))).isEqualTo(Domain.none(BIGINT));

        assertThat(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true).intersect(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), true))).isEqualTo(Domain.onlyNull(BIGINT));

        assertThat(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true).intersect(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))).isEqualTo(Domain.singleValue(BIGINT, 1L));
    }

    @Test
    public void testUnion()
    {
        assertUnion(Domain.all(BIGINT), Domain.all(BIGINT), Domain.all(BIGINT));
        assertUnion(Domain.none(BIGINT), Domain.none(BIGINT), Domain.none(BIGINT));
        assertUnion(Domain.all(BIGINT), Domain.none(BIGINT), Domain.all(BIGINT));
        assertUnion(Domain.notNull(BIGINT), Domain.onlyNull(BIGINT), Domain.all(BIGINT));
        assertUnion(Domain.singleValue(BIGINT, 0L), Domain.all(BIGINT), Domain.all(BIGINT));
        assertUnion(Domain.singleValue(BIGINT, 0L), Domain.notNull(BIGINT), Domain.notNull(BIGINT));
        assertUnion(Domain.singleValue(BIGINT, 0L), Domain.onlyNull(BIGINT), Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 0L)), true));

        assertUnion(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true),
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), true),
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), true));

        assertUnion(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true),
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false),
                Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), true));

        assertUnion(
                Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 20L)), true),
                Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 10L)), true),
                Domain.all(BIGINT));

        assertUnion(
                Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 20L)), false),
                Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 10L)), false),
                Domain.create(ValueSet.all(BIGINT), false));
    }

    @Test
    public void testSubtract()
    {
        assertThat(Domain.all(BIGINT).subtract(Domain.all(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.all(BIGINT).subtract(Domain.none(BIGINT))).isEqualTo(Domain.all(BIGINT));
        assertThat(Domain.all(BIGINT).subtract(Domain.notNull(BIGINT))).isEqualTo(Domain.onlyNull(BIGINT));
        assertThat(Domain.all(BIGINT).subtract(Domain.onlyNull(BIGINT))).isEqualTo(Domain.notNull(BIGINT));
        assertThat(Domain.all(BIGINT).subtract(Domain.singleValue(BIGINT, 0L))).isEqualTo(Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 0L)), true));

        assertThat(Domain.none(BIGINT).subtract(Domain.all(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.none(BIGINT).subtract(Domain.none(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.none(BIGINT).subtract(Domain.notNull(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.none(BIGINT).subtract(Domain.onlyNull(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.none(BIGINT).subtract(Domain.singleValue(BIGINT, 0L))).isEqualTo(Domain.none(BIGINT));

        assertThat(Domain.notNull(BIGINT).subtract(Domain.all(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.notNull(BIGINT).subtract(Domain.none(BIGINT))).isEqualTo(Domain.notNull(BIGINT));
        assertThat(Domain.notNull(BIGINT).subtract(Domain.notNull(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.notNull(BIGINT).subtract(Domain.onlyNull(BIGINT))).isEqualTo(Domain.notNull(BIGINT));
        assertThat(Domain.notNull(BIGINT).subtract(Domain.singleValue(BIGINT, 0L))).isEqualTo(Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 0L)), false));

        assertThat(Domain.onlyNull(BIGINT).subtract(Domain.all(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.onlyNull(BIGINT).subtract(Domain.none(BIGINT))).isEqualTo(Domain.onlyNull(BIGINT));
        assertThat(Domain.onlyNull(BIGINT).subtract(Domain.notNull(BIGINT))).isEqualTo(Domain.onlyNull(BIGINT));
        assertThat(Domain.onlyNull(BIGINT).subtract(Domain.onlyNull(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.onlyNull(BIGINT).subtract(Domain.singleValue(BIGINT, 0L))).isEqualTo(Domain.onlyNull(BIGINT));

        assertThat(Domain.singleValue(BIGINT, 0L).subtract(Domain.all(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.singleValue(BIGINT, 0L).subtract(Domain.none(BIGINT))).isEqualTo(Domain.singleValue(BIGINT, 0L));
        assertThat(Domain.singleValue(BIGINT, 0L).subtract(Domain.notNull(BIGINT))).isEqualTo(Domain.none(BIGINT));
        assertThat(Domain.singleValue(BIGINT, 0L).subtract(Domain.onlyNull(BIGINT))).isEqualTo(Domain.singleValue(BIGINT, 0L));
        assertThat(Domain.singleValue(BIGINT, 0L).subtract(Domain.singleValue(BIGINT, 0L))).isEqualTo(Domain.none(BIGINT));

        assertThat(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true).subtract(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), true))).isEqualTo(Domain.singleValue(BIGINT, 1L));

        assertThat(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true).subtract(Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))).isEqualTo(Domain.onlyNull(BIGINT));
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

        ObjectMapper mapper = new ObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)));

        Domain domain = Domain.all(BIGINT);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.none(DOUBLE);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.notNull(BOOLEAN);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.notNull(HYPER_LOG_LOG);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.onlyNull(VARCHAR);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.onlyNull(HYPER_LOG_LOG);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.singleValue(BIGINT, Long.MIN_VALUE);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.singleValue(ID, Long.MIN_VALUE);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 0L), Range.equal(BIGINT, 1L), Range.range(BIGINT, 2L, true, 3L, true)), true);
        assertThat(domain).isEqualTo(mapper.readValue(mapper.writeValueAsString(domain), Domain.class));
    }

    private void assertUnion(Domain first, Domain second, Domain expected)
    {
        assertThat(first.union(second)).isEqualTo(expected);
        assertThat(Domain.union(ImmutableList.of(first, second))).isEqualTo(expected);
    }
}
