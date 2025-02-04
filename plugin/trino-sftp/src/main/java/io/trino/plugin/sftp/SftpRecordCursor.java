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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SftpRecordCursor
        implements RecordCursor
{
    private final InputStream inputStream;
    private final List<? extends ColumnHandle> columns;
    private final long totalBytes;
    private final List<List<Object>> records;
    private int currentRecordIndex;
    private List<Object> currentRecord;
    private final long startTime;

    public SftpRecordCursor(
            CsvParser parser,
            InputStream inputStream,
            List<? extends ColumnHandle> columns,
            long totalBytes)
    {
        requireNonNull(parser, "parser is null");
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.totalBytes = totalBytes;
        this.startTime = System.nanoTime();

        // Parse all records at initialization
        this.records = parser.parse(inputStream);
        this.currentRecordIndex = -1;
        this.currentRecord = null;
    }

    @Override
    public long getCompletedBytes()
    {
        if (currentRecordIndex < 0) {
            return 0;
        }
        // Approximate bytes read based on current record position
        return (long) ((double) currentRecordIndex / records.size() * totalBytes);
    }

    @Override
    public long getReadTimeNanos()
    {
        return System.nanoTime() - startTime;
    }

    @Override
    public Type getType(int field)
    {
        return ((SftpColumnHandle) columns.get(field)).getType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        currentRecordIndex++;
        if (currentRecordIndex < records.size()) {
            currentRecord = records.get(currentRecordIndex);
            return true;
        }
        return false;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkState(currentRecord != null, "No current record");
        Object value = currentRecord.get(field);
        if (value == null) {
            return false;
        }
        return (Boolean) value;
    }

    @Override
    public long getLong(int field)
    {
        checkState(currentRecord != null, "No current record");
        Object value = currentRecord.get(field);
        if (value == null) {
            return 0;
        }
        if (value instanceof Integer) {
            return ((Integer) value).longValue();
        }
        return (Long) value;
    }

    @Override
    public double getDouble(int field)
    {
        checkState(currentRecord != null, "No current record");
        Object value = currentRecord.get(field);
        if (value == null) {
            return 0.0;
        }
        return (Double) value;
    }

    @Override
    public Slice getSlice(int field)
    {
        checkState(currentRecord != null, "No current record");
        Object value = currentRecord.get(field);
        if (value == null) {
            return null;
        }
        return Slices.utf8Slice(value.toString());
    }

    @Override
    public Object getObject(int field)
    {
        checkState(currentRecord != null, "No current record");
        return currentRecord.get(field);
    }

    @Override
    public boolean isNull(int field)
    {
        checkState(currentRecord != null, "No current record");
        return currentRecord.get(field) == null;
    }

    @Override
    public void close()
    {
        try {
            inputStream.close();
        }
        catch (IOException e) {
            throw new RuntimeException("Error closing input stream", e);
        }
    }
}
