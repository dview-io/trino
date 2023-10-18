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
package io.trino.plugin.dview.page;

import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.reader.ParquetReader;
import io.trino.plugin.dview.DviewErrorCode;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.OptionalLong;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DviewParquetPageSource
        implements ConnectorPageSource
{
    private final ParquetReader parquetReader;
    private boolean closed;
    private long completedPositions;

    public DviewParquetPageSource(ParquetReader parquetReader)
    {
        this.parquetReader = requireNonNull(parquetReader, "parquetReader is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return parquetReader.getDataSource().getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return parquetReader.getDataSource().getReadTimeNanos();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page page = parquetReader.nextPage();
//            System.out.println(page);
            if (closed || page == null) {
                close();
                return null;
            }
            completedPositions += page.getPositionCount();
            return page;
        }
        catch (IOException | RuntimeException e) {
//            closeAllSuppress(e, this);
            throw handleException(parquetReader.getDataSource().getId(), e);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return parquetReader.getMemoryContext().getBytes();
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            parquetReader.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static TrinoException handleException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        if (exception instanceof ParquetCorruptionException) {
            return new TrinoException(DviewErrorCode.DVIEW_BAD_DATA, exception);
        }
        return new TrinoException(DviewErrorCode.DVIEW_CURSOR_ERROR, format("Failed to read Parquet file: %s", dataSourceId), exception);
    }
}
