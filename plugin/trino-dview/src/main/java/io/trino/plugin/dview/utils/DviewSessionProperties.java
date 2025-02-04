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
package io.trino.plugin.dview.utils;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;

public final class DviewSessionProperties
        implements SessionPropertiesProvider
{
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_WRITER_BLOCK_SIZE = "parquet_writer_block_size";
    private static final String PARQUET_WRITER_PAGE_SIZE = "parquet_writer_page_size";
    private static final String PARQUET_USE_BLOOM_FILTER = "parquet_use_bloom_filter";
    private static final String PARQUET_SMALL_FILE_THRESHOLD = "parquet_small_file_threshold";
    private static final String PARQUET_WRITER_BATCH_SIZE = "parquet_writer_batch_size";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public DviewSessionProperties(ParquetReaderConfig parquetReaderConfig, ParquetWriterConfig parquetWriterConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(dataSizeProperty(
                        PARQUET_MAX_READ_BLOCK_SIZE,
                        "Parquet: Maximum size of a block to read",
                        parquetReaderConfig.getMaxReadBlockSize(),
                        false))
                .add(dataSizeProperty(
                        PARQUET_WRITER_BLOCK_SIZE,
                        "Parquet: Writer block size",
                        parquetWriterConfig.getBlockSize(),
                        false))
                .add(dataSizeProperty(
                        PARQUET_WRITER_PAGE_SIZE,
                        "Parquet: Writer page size",
                        parquetWriterConfig.getPageSize(),
                        value -> {
                            if (value.toBytes() < 4096) {
                                throw new TrinoException(INVALID_SESSION_PROPERTY, "Page size must be at least 4096 bytes");
                            }
                        },
                        false))
                .add(booleanProperty(
                        PARQUET_USE_BLOOM_FILTER,
                        "Use Parquet Bloom filters",
                        parquetReaderConfig.isUseBloomFilter(),
                        false))
                .add(dataSizeProperty(
                        PARQUET_SMALL_FILE_THRESHOLD,
                        "Parquet: Size below which a parquet file will be read entirely",
                        parquetReaderConfig.getSmallFileThreshold(),
                        false))
                .add(integerProperty(
                        PARQUET_WRITER_BATCH_SIZE,
                        "Parquet: Maximum number of rows passed to the writer in each batch",
                        parquetWriterConfig.getBatchSize(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static DataSize getParquetMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_MAX_READ_BLOCK_SIZE, DataSize.class);
    }

    public static DataSize getParquetWriterBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_BLOCK_SIZE, DataSize.class);
    }

    public static DataSize getParquetWriterPageSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_PAGE_SIZE, DataSize.class);
    }

    public static boolean isParquetUseBloomFilter(ConnectorSession session)
    {
        return session.getProperty(PARQUET_USE_BLOOM_FILTER, Boolean.class);
    }

    public static DataSize getParquetSmallFileThreshold(ConnectorSession session)
    {
        return session.getProperty(PARQUET_SMALL_FILE_THRESHOLD, DataSize.class);
    }

    public static int getParquetBatchSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_BATCH_SIZE, Integer.class);
    }
}
