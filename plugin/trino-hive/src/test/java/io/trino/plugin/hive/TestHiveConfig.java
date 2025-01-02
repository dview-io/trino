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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.hive.HiveSessionProperties.InsertExistingPartitionsBehavior.APPEND;
import static io.trino.plugin.hive.HiveSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE;
import static io.trino.plugin.hive.util.TestHiveUtil.nonDefaultTimeZone;

public class TestHiveConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HiveConfig.class)
                .setSingleStatementWritesOnly(false)
                .setMaxSplitSize(DataSize.of(64, Unit.MEGABYTE))
                .setMaxPartitionsPerScan(1_000_000)
                .setMaxPartitionsForEagerLoad(100_000)
                .setMaxOutstandingSplits(3_000)
                .setMaxOutstandingSplitsSize(DataSize.of(256, Unit.MEGABYTE))
                .setMaxSplitIteratorThreads(1_000)
                .setPerTransactionMetastoreCacheMaximumSize(1000)
                .setMinPartitionBatchSize(10)
                .setMaxPartitionBatchSize(100)
                .setMaxInitialSplits(200)
                .setMaxInitialSplitSize(DataSize.of(32, Unit.MEGABYTE))
                .setSplitLoaderConcurrency(64)
                .setMaxSplitsPerSecond(null)
                .setDomainCompactionThreshold(1000)
                .setTargetMaxFileSize(DataSize.of(1, GIGABYTE))
                .setIdleWriterMinFileSize(DataSize.of(16, MEGABYTE))
                .setForceLocalScheduling(false)
                .setMaxConcurrentFileSystemOperations(20)
                .setMaxConcurrentMetastoreDrops(20)
                .setMaxConcurrentMetastoreUpdates(20)
                .setMaxPartitionDropsPerQuery(100_000)
                .setRecursiveDirWalkerEnabled(false)
                .setIgnoreAbsentPartitions(false)
                .setHiveStorageFormat(HiveStorageFormat.ORC)
                .setHiveCompressionCodec(HiveCompressionOption.GZIP)
                .setRespectTableFormat(true)
                .setImmutablePartitions(false)
                .setInsertExistingPartitionsBehavior(APPEND)
                .setCreateEmptyBucketFiles(false)
                .setDeleteSchemaLocationsFallback(false)
                .setSortedWritingEnabled(true)
                .setPropagateTableScanSortingProperties(false)
                .setMaxPartitionsPerWriter(100)
                .setWriteValidationThreads(16)
                .setValidateBucketing(true)
                .setParallelPartitionedBucketedWrites(true)
                .setTextMaxLineLength(DataSize.of(100, Unit.MEGABYTE))
                .setOrcLegacyTimeZone(TimeZone.getDefault().getID())
                .setParquetTimeZone(TimeZone.getDefault().getID())
                .setUseParquetColumnNames(true)
                .setRcfileTimeZone(TimeZone.getDefault().getID())
                .setRcfileWriterValidate(false)
                .setSkipDeletionForAlter(false)
                .setSkipTargetCleanupOnRollback(false)
                .setBucketExecutionEnabled(true)
                .setTableStatisticsEnabled(true)
                .setOptimizeMismatchedBucketCount(true)
                .setWritesToNonManagedTablesEnabled(false)
                .setCreatesOfNonManagedTablesEnabled(true)
                .setPartitionStatisticsSampleSize(100)
                .setIgnoreCorruptedStatistics(false)
                .setCollectColumnStatisticsOnWrite(true)
                .setTemporaryStagingDirectoryEnabled(true)
                .setTemporaryStagingDirectoryPath("/tmp/presto-${USER}")
                .setDelegateTransactionalManagedTableLocationToMetastore(false)
                .setFileStatusCacheExpireAfterWrite(new Duration(1, TimeUnit.MINUTES))
                .setFileStatusCacheMaxRetainedSize(DataSize.of(1, GIGABYTE))
                .setFileStatusCacheTables(ImmutableList.of())
                .setPerTransactionFileStatusCacheMaxRetainedSize(DataSize.of(100, MEGABYTE))
                .setTranslateHiveViews(false)
                .setLegacyHiveViewTranslation(false)
                .setHiveViewsRunAsInvoker(false)
                .setHiveTransactionHeartbeatInterval(null)
                .setHiveTransactionHeartbeatThreads(5)
                .setAllowRegisterPartition(false)
                .setQueryPartitionFilterRequired(false)
                .setQueryPartitionFilterRequiredSchemas(ImmutableList.of())
                .setProjectionPushdownEnabled(true)
                .setDynamicFilteringWaitTimeout(new Duration(0, TimeUnit.MINUTES))
                .setTimestampPrecision(HiveTimestampPrecision.DEFAULT_PRECISION)
                .setIcebergCatalogName(null)
                .setSizeBasedSplitWeightsEnabled(true)
                .setMinimumAssignedSplitWeight(0.05)
                .setDeltaLakeCatalogName(null)
                .setHudiCatalogName(null)
                .setAutoPurge(false)
                .setPartitionProjectionEnabled(false)
                .setS3StorageClassFilter(S3StorageClassFilter.READ_ALL)
                .setMetadataParallelism(8));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.single-statement-writes", "true")
                .put("hive.max-split-size", "256MB")
                .put("hive.max-partitions-per-scan", "123")
                .put("hive.max-partitions-for-eager-load", "122")
                .put("hive.max-outstanding-splits", "10")
                .put("hive.max-outstanding-splits-size", "32MB")
                .put("hive.max-split-iterator-threads", "10")
                .put("hive.per-transaction-metastore-cache-maximum-size", "500")
                .put("hive.metastore.partition-batch-size.min", "1")
                .put("hive.metastore.partition-batch-size.max", "1000")
                .put("hive.max-initial-splits", "10")
                .put("hive.max-initial-split-size", "16MB")
                .put("hive.split-loader-concurrency", "1")
                .put("hive.max-splits-per-second", "1")
                .put("hive.domain-compaction-threshold", "42")
                .put("hive.target-max-file-size", "72MB")
                .put("hive.idle-writer-min-file-size", "1MB")
                .put("hive.recursive-directories", "true")
                .put("hive.ignore-absent-partitions", "true")
                .put("hive.storage-format", "SEQUENCEFILE")
                .put("hive.compression-codec", "NONE")
                .put("hive.respect-table-format", "false")
                .put("hive.immutable-partitions", "true")
                .put("hive.insert-existing-partitions-behavior", "OVERWRITE")
                .put("hive.create-empty-bucket-files", "true")
                .put("hive.delete-schema-locations-fallback", "true")
                .put("hive.max-partitions-per-writers", "222")
                .put("hive.write-validation-threads", "11")
                .put("hive.validate-bucketing", "false")
                .put("hive.parallel-partitioned-bucketed-writes", "false")
                .put("hive.force-local-scheduling", "true")
                .put("hive.max-concurrent-file-system-operations", "100")
                .put("hive.max-concurrent-metastore-drops", "100")
                .put("hive.max-concurrent-metastore-updates", "100")
                .put("hive.max-partition-drops-per-query", "1000")
                .put("hive.text.max-line-length", "13MB")
                .put("hive.orc.time-zone", nonDefaultTimeZone().getID())
                .put("hive.parquet.time-zone", nonDefaultTimeZone().getID())
                .put("hive.parquet.use-column-names", "false")
                .put("hive.rcfile.time-zone", nonDefaultTimeZone().getID())
                .put("hive.rcfile.writer.validate", "true")
                .put("hive.skip-deletion-for-alter", "true")
                .put("hive.skip-target-cleanup-on-rollback", "true")
                .put("hive.bucket-execution", "false")
                .put("hive.sorted-writing", "false")
                .put("hive.propagate-table-scan-sorting-properties", "true")
                .put("hive.table-statistics-enabled", "false")
                .put("hive.optimize-mismatched-bucket-count", "false")
                .put("hive.non-managed-table-writes-enabled", "true")
                .put("hive.non-managed-table-creates-enabled", "false")
                .put("hive.partition-statistics-sample-size", "1234")
                .put("hive.ignore-corrupted-statistics", "true")
                .put("hive.collect-column-statistics-on-write", "false")
                .put("hive.temporary-staging-directory-enabled", "false")
                .put("hive.temporary-staging-directory-path", "updated")
                .put("hive.delegate-transactional-managed-table-location-to-metastore", "true")
                .put("hive.file-status-cache-tables", "foo.bar1, foo.bar2")
                .put("hive.file-status-cache.max-retained-size", "1000B")
                .put("hive.file-status-cache-expire-time", "30m")
                .put("hive.per-transaction-file-status-cache.max-retained-size", "42B")
                .put("hive.hive-views.enabled", "true")
                .put("hive.hive-views.legacy-translation", "true")
                .put("hive.hive-views.run-as-invoker", "true")
                .put("hive.transaction-heartbeat-interval", "10s")
                .put("hive.transaction-heartbeat-threads", "10")
                .put("hive.allow-register-partition-procedure", "true")
                .put("hive.query-partition-filter-required", "true")
                .put("hive.query-partition-filter-required-schemas", "foo, bar")
                .put("hive.projection-pushdown-enabled", "false")
                .put("hive.dynamic-filtering.wait-timeout", "10s")
                .put("hive.timestamp-precision", "NANOSECONDS")
                .put("hive.iceberg-catalog-name", "iceberg")
                .put("hive.size-based-split-weights-enabled", "false")
                .put("hive.minimum-assigned-split-weight", "1.0")
                .put("hive.delta-lake-catalog-name", "delta")
                .put("hive.hudi-catalog-name", "hudi")
                .put("hive.auto-purge", "true")
                .put("hive.partition-projection-enabled", "true")
                .put("hive.s3.storage-class-filter", "READ_NON_GLACIER_AND_RESTORED")
                .put("hive.metadata.parallelism", "10")
                .buildOrThrow();

        HiveConfig expected = new HiveConfig()
                .setSingleStatementWritesOnly(true)
                .setMaxSplitSize(DataSize.of(256, Unit.MEGABYTE))
                .setMaxPartitionsPerScan(123)
                .setMaxPartitionsForEagerLoad(122)
                .setMaxOutstandingSplits(10)
                .setMaxOutstandingSplitsSize(DataSize.of(32, Unit.MEGABYTE))
                .setMaxSplitIteratorThreads(10)
                .setPerTransactionMetastoreCacheMaximumSize(500)
                .setMinPartitionBatchSize(1)
                .setMaxPartitionBatchSize(1000)
                .setMaxInitialSplits(10)
                .setMaxInitialSplitSize(DataSize.of(16, Unit.MEGABYTE))
                .setSplitLoaderConcurrency(1)
                .setMaxSplitsPerSecond(1)
                .setDomainCompactionThreshold(42)
                .setTargetMaxFileSize(DataSize.of(72, Unit.MEGABYTE))
                .setIdleWriterMinFileSize(DataSize.of(1, MEGABYTE))
                .setForceLocalScheduling(true)
                .setMaxConcurrentFileSystemOperations(100)
                .setMaxConcurrentMetastoreDrops(100)
                .setMaxConcurrentMetastoreUpdates(100)
                .setMaxPartitionDropsPerQuery(1000)
                .setRecursiveDirWalkerEnabled(true)
                .setIgnoreAbsentPartitions(true)
                .setHiveStorageFormat(HiveStorageFormat.SEQUENCEFILE)
                .setHiveCompressionCodec(HiveCompressionOption.NONE)
                .setRespectTableFormat(false)
                .setImmutablePartitions(true)
                .setInsertExistingPartitionsBehavior(OVERWRITE)
                .setCreateEmptyBucketFiles(true)
                .setDeleteSchemaLocationsFallback(true)
                .setMaxPartitionsPerWriter(222)
                .setWriteValidationThreads(11)
                .setValidateBucketing(false)
                .setParallelPartitionedBucketedWrites(false)
                .setTextMaxLineLength(DataSize.of(13, Unit.MEGABYTE))
                .setOrcLegacyTimeZone(nonDefaultTimeZone().getID())
                .setParquetTimeZone(nonDefaultTimeZone().getID())
                .setUseParquetColumnNames(false)
                .setRcfileTimeZone(nonDefaultTimeZone().getID())
                .setRcfileWriterValidate(true)
                .setSkipDeletionForAlter(true)
                .setSkipTargetCleanupOnRollback(true)
                .setBucketExecutionEnabled(false)
                .setSortedWritingEnabled(false)
                .setPropagateTableScanSortingProperties(true)
                .setTableStatisticsEnabled(false)
                .setOptimizeMismatchedBucketCount(false)
                .setWritesToNonManagedTablesEnabled(true)
                .setCreatesOfNonManagedTablesEnabled(false)
                .setPartitionStatisticsSampleSize(1234)
                .setIgnoreCorruptedStatistics(true)
                .setCollectColumnStatisticsOnWrite(false)
                .setTemporaryStagingDirectoryEnabled(false)
                .setTemporaryStagingDirectoryPath("updated")
                .setDelegateTransactionalManagedTableLocationToMetastore(true)
                .setFileStatusCacheTables(ImmutableList.of("foo.bar1", "foo.bar2"))
                .setFileStatusCacheMaxRetainedSize(DataSize.ofBytes(1000))
                .setFileStatusCacheExpireAfterWrite(new Duration(30, TimeUnit.MINUTES))
                .setPerTransactionFileStatusCacheMaxRetainedSize(DataSize.ofBytes(42))
                .setTranslateHiveViews(true)
                .setLegacyHiveViewTranslation(true)
                .setHiveViewsRunAsInvoker(true)
                .setHiveTransactionHeartbeatInterval(new Duration(10, TimeUnit.SECONDS))
                .setHiveTransactionHeartbeatThreads(10)
                .setAllowRegisterPartition(true)
                .setQueryPartitionFilterRequired(true)
                .setQueryPartitionFilterRequiredSchemas(ImmutableList.of("foo", "bar"))
                .setProjectionPushdownEnabled(false)
                .setDynamicFilteringWaitTimeout(new Duration(10, TimeUnit.SECONDS))
                .setTimestampPrecision(HiveTimestampPrecision.NANOSECONDS)
                .setIcebergCatalogName("iceberg")
                .setSizeBasedSplitWeightsEnabled(false)
                .setMinimumAssignedSplitWeight(1.0)
                .setDeltaLakeCatalogName("delta")
                .setHudiCatalogName("hudi")
                .setAutoPurge(true)
                .setPartitionProjectionEnabled(true)
                .setS3StorageClassFilter(S3StorageClassFilter.READ_NON_GLACIER_AND_RESTORED)
                .setMetadataParallelism(10);

        assertFullMapping(properties, expected);
    }
}
