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
package io.trino.cost;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.lang.Math.floor;
import static java.lang.Math.pow;

/**
 * Makes stats consistent
 */
public class StatsNormalizer
{
    public PlanNodeStatsEstimate normalize(PlanNodeStatsEstimate stats)
    {
        return normalize(stats, Optional.empty());
    }

    public PlanNodeStatsEstimate normalize(PlanNodeStatsEstimate stats, Collection<Symbol> outputSymbols)
    {
        return normalize(stats, Optional.of(outputSymbols));
    }

    private PlanNodeStatsEstimate normalize(PlanNodeStatsEstimate stats, Optional<Collection<Symbol>> outputSymbols)
    {
        PlanNodeStatsEstimate.Builder normalized = PlanNodeStatsEstimate.buildFrom(stats);

        Predicate<Symbol> symbolFilter = outputSymbols
                .map(ImmutableSet::copyOf)
                .map(set -> (Predicate<Symbol>) set::contains)
                .orElse(symbol -> true);

        for (Symbol symbol : stats.getSymbolsWithKnownStatistics()) {
            if (!symbolFilter.test(symbol)) {
                normalized.removeSymbolStatistics(symbol);
                continue;
            }

            SymbolStatsEstimate symbolStats = stats.getSymbolStatistics(symbol);
            SymbolStatsEstimate normalizedSymbolStats = stats.isOutputRowCountUnknown()
                    ? normalizeSymbolStatsWithoutRowCount(symbol, symbolStats)
                    : normalizeSymbolStats(symbol, symbolStats, stats);

            if (normalizedSymbolStats.isUnknown()) {
                normalized.removeSymbolStatistics(symbol);
                continue;
            }
            if (!Objects.equals(normalizedSymbolStats, symbolStats)) {
                normalized.addSymbolStatistics(symbol, normalizedSymbolStats);
            }
        }

        return normalized.build();
    }

    /**
     * Calculates consistent stats for a symbol when row count is unavailable.
     */
    private SymbolStatsEstimate normalizeSymbolStatsWithoutRowCount(Symbol symbol, SymbolStatsEstimate symbolStats)
    {
        if (symbolStats.isUnknown()) {
            return SymbolStatsEstimate.unknown();
        }
        double distinctValuesCount = symbolStats.getDistinctValuesCount();

        if (!isNaN(distinctValuesCount)) {
            double maxDistinctValuesByLowHigh = maxDistinctValuesByLowHigh(symbolStats, symbol.type());
            if (distinctValuesCount > maxDistinctValuesByLowHigh) {
                distinctValuesCount = maxDistinctValuesByLowHigh;
            }
        }

        if (distinctValuesCount == 0.0) {
            return SymbolStatsEstimate.zero();
        }

        return SymbolStatsEstimate.buildFrom(symbolStats)
                .setDistinctValuesCount(distinctValuesCount)
                .build();
    }

    /**
     * Calculates consistent stats for a symbol when row count is available.
     */
    private SymbolStatsEstimate normalizeSymbolStats(Symbol symbol, SymbolStatsEstimate symbolStats, PlanNodeStatsEstimate stats)
    {
        if (stats.getOutputRowCount() == 0) {
            return SymbolStatsEstimate.zero();
        }

        if (symbolStats.isUnknown()) {
            return SymbolStatsEstimate.unknown();
        }

        double outputRowCount = stats.getOutputRowCount();
        checkArgument(outputRowCount > 0, "outputRowCount must be greater than zero: %s", outputRowCount);
        double distinctValuesCount = symbolStats.getDistinctValuesCount();
        double nullsFraction = symbolStats.getNullsFraction();

        if (!isNaN(distinctValuesCount)) {
            double maxDistinctValuesByLowHigh = maxDistinctValuesByLowHigh(symbolStats, symbol.type());
            if (distinctValuesCount > maxDistinctValuesByLowHigh) {
                distinctValuesCount = maxDistinctValuesByLowHigh;
            }

            if (distinctValuesCount > outputRowCount) {
                distinctValuesCount = outputRowCount;
            }

            double nonNullValues = outputRowCount * (1 - nullsFraction);
            if (distinctValuesCount > nonNullValues) {
                double difference = distinctValuesCount - nonNullValues;
                distinctValuesCount -= difference / 2;
                nonNullValues += difference / 2;
                nullsFraction = 1 - nonNullValues / outputRowCount;
            }
        }

        if (distinctValuesCount == 0.0) {
            return SymbolStatsEstimate.zero();
        }

        return SymbolStatsEstimate.buildFrom(symbolStats)
                .setDistinctValuesCount(distinctValuesCount)
                .setNullsFraction(nullsFraction)
                .build();
    }

    private double maxDistinctValuesByLowHigh(SymbolStatsEstimate symbolStats, Type type)
    {
        if (symbolStats.statisticRange().length() == 0.0) {
            return 1;
        }

        if (!isDiscrete(type)) {
            return NaN;
        }

        double length = symbolStats.getHighValue() - symbolStats.getLowValue();
        if (isNaN(length)) {
            return NaN;
        }

        if (type instanceof DecimalType decimalType) {
            length *= pow(10, decimalType.getScale());
        }
        return floor(length + 1);
    }

    private static boolean isDiscrete(Type type)
    {
        return type.equals(IntegerType.INTEGER) ||
                type.equals(BigintType.BIGINT) ||
                type.equals(SmallintType.SMALLINT) ||
                type.equals(TinyintType.TINYINT) ||
                type.equals(BooleanType.BOOLEAN) ||
                type.equals(DateType.DATE) ||
                type instanceof DecimalType;
    }
}
