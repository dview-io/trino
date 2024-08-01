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

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.dview.client.DviewClient;
import io.trino.plugin.hive.parquet.ParquetFileWriter;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.MessageType;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.readBigDecimal;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.floorDiv;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class DviewPageSink
        implements ConnectorPageSink
{
    private static final Logger log = Logger.get(DviewPageSink.class);
    private final List<WriteSupport.WriteContext> writers = new ArrayList<>();
    private final Collection<Slice> commitTasks = new ArrayList<>();
    private final List<Type> columnTypes;
    private final List<String> columnNames;
    private final Set<String> outputFilePaths = new HashSet<>();
    private final Map<String, ParquetFileWriter> dthrMap = new HashMap<>();
    private final TrinoOutputFile baseOutputFile;
    private final Closeable rollbackAction;
    private final ParquetWriterOptions parquetWriterOptions;
    private final int[] fileInputColumnIndexes;
    private final CompressionCodec compressionCodec;
    private final String trinoVersion;
    private final MessageType messageType;
    private final Map<List<String>, Type> primitiveTypes;
    private final TrinoFileSystem fileSytem;
    private final long entityId;
    private DviewClient dviewClient;

    public DviewPageSink(
            DviewClient dviewClient,
            TrinoFileSystem fileSytem,
            TrinoOutputFile outputFile,
            Closeable rollbackAction,
            List<Type> fileColumnTypes,
            List<String> fileColumnNames,
            MessageType messageType,
            Map<List<String>, Type> primitiveTypes,
            ParquetWriterOptions parquetWriterOptions,
            int[] fileInputColumnIndexes,
            CompressionCodec compressionCodec,
            String trinoVersion,
            long entityId)
            throws IOException
    {
        this.dviewClient = dviewClient;
        this.baseOutputFile = outputFile;
        this.rollbackAction = rollbackAction;
        this.columnTypes = fileColumnTypes;
        this.columnNames = fileColumnNames;
        this.parquetWriterOptions = parquetWriterOptions;
        this.fileInputColumnIndexes = fileInputColumnIndexes;
        this.compressionCodec = compressionCodec;
        this.trinoVersion = trinoVersion;
        this.messageType = messageType;
        this.primitiveTypes = primitiveTypes;
        this.fileSytem = fileSytem;
        this.entityId = entityId;
    }

    @Override
    public long getCompletedBytes()
    {
        long sum = 0;
        for (Map.Entry<String, ParquetFileWriter> entry : dthrMap.entrySet()) {
            sum += entry.getValue().getWrittenBytes();
        }
        return sum;
    }

    @Override
    public long getMemoryUsage()
    {
        long sum = 0;
        for (Map.Entry<String, ParquetFileWriter> entry : dthrMap.entrySet()) {
            sum += entry.getValue().getMemoryUsage();
        }
        return sum;
    }

    @Override
    public long getValidationCpuNanos()
    {
        long sum = 0;
        for (Map.Entry<String, ParquetFileWriter> entry : dthrMap.entrySet()) {
            sum += entry.getValue().getValidationCpuNanos();
        }
        return sum;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        log.info("Entering into appendPage");
        doAppend(page);
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        log.info("Entering into finish");
        dthrMap.forEach((k, v) -> v.commit());
        dviewClient.insertIntoDocument(outputFilePaths, entityId);
        outputFilePaths.clear();
        dthrMap.clear();
        writers.clear();
        return completedFuture(commitTasks);
    }

    @Override
    public void abort()
    {
        dthrMap.forEach((k, v) -> v.rollback());
    }

    private void doAppend(Page page)
    {
        writePage(page);
    }

    private void writePage(Page page)
    {
        log.info("Entering into writePage");
        try {
            pageValues(page);
        }
        catch (Exception e) {
            throw new RuntimeException("Error writing page to Parquet file", e);
        }
    }

    private void pageValues(Page page)
    {
        log.info("Entering into pageValues");
        int positionCount = page.getPositionCount();
        int channelCount = page.getChannelCount();
        Map<Integer, String> positionCountKey = new HashMap<>();
        Map<String, Integer> keyDeclareCount = new HashMap<>();
        Map<String, PageBuilder> pageBuilderMap = new HashMap<>();
        for (int position = 0; position < positionCount; position++) {
            Object dt = null;
            Object hr = null;

            for (int colIndex = 0; colIndex < channelCount; colIndex++) {
                Block block = page.getBlock(colIndex);
                if (this.columnNames.get(colIndex).equals("dpt_updatedat")) {
                    dt = getObjectValue(this.columnTypes.get(colIndex), block, position);
                }
                else if (this.columnNames.get(colIndex).equals("hr")) {
                    hr = getObjectValue(this.columnTypes.get(colIndex), block, position);
                }
            }

            String key;
            if (dt != null) {
                if (hr != null) {
                    key = "dt=" + dt + "/hr=" + hr;
                }
                else {
                    key = "dt=" + dt;
                }
            }
            else {
                key = "";
            }
            positionCountKey.put(position, key);
            Integer value = keyDeclareCount.getOrDefault(key, 0);
            keyDeclareCount.put(key, value + 1);
        }
        keyDeclareCount.forEach((key, value) ->
        {
            PageBuilder pageBuilder = pageBuilderMap.getOrDefault(key, new PageBuilder(columnTypes));
            pageBuilder.declarePositions(value);
            pageBuilderMap.put(key, pageBuilder);
        });
        for (int position = 0; position < positionCount; position++) {
            String key = positionCountKey.get(position);
            PageBuilder pageBuilder = pageBuilderMap.get(key);
            for (int colIndex = 0; colIndex < channelCount; colIndex++) {
                Block block = page.getBlock(colIndex);
                getValueBlock(columnTypes.get(colIndex), block, pageBuilder.getBlockBuilder(colIndex), position);
                pageBuilderMap.put(key, pageBuilder);
            }
        }
        pageBuilderMap.forEach((k, pageBuilder) -> {
            String outputPath = baseOutputFile.location().toString();
            Location location;
            if (k.isEmpty()) {
                location = Location.of(outputPath + File.separator + System.currentTimeMillis() + ".parquet");
            }
            else {
                location = Location.of(outputPath + File.separator + k + File.separator + System.currentTimeMillis() + ".parquet");
            }

            outputFilePaths.add(location.toString());

            if (dthrMap.containsKey(location.path())) {
                ParquetFileWriter existingParquetFileWriter = dthrMap.get(location.path());
                existingParquetFileWriter.appendRows(pageBuilder.build());
            }
            else {
                TrinoOutputFile newOutputFile = fileSytem.newOutputFile(location);
                try {
                    ParquetFileWriter newParquetFileWriter = new ParquetFileWriter(
                            newOutputFile,
                            rollbackAction,
                            columnTypes,
                            columnNames,
                            messageType,
                            primitiveTypes,
                            parquetWriterOptions,
                            fileInputColumnIndexes,
                            compressionCodec,
                            trinoVersion,
                            Optional.empty(),
                            Optional.empty());
                    newParquetFileWriter.appendRows(pageBuilder.build());
                    dthrMap.put(location.path(), newParquetFileWriter);
                }
                catch (IOException e) {
                    log.error("Error while writing into new ParquetFileWriter in pageValues::ParquetFileWriter");
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private void getValueBlock(Type type, Block block, BlockBuilder blockBuilder, int position)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
            return;
        }

        Object value = getObjectValue(type, block, position);

        if (type.equals(BOOLEAN)) {
            BOOLEAN.writeBoolean(blockBuilder, value != null && (boolean) value);
        }
        else if (type.equals(BIGINT)) {
            BIGINT.writeLong(blockBuilder, value == null ? 0L : (long) value);
        }
        else if (type.equals(INTEGER)) {
            INTEGER.writeInt(blockBuilder, value == null ? 0 : (int) value);
        }
        else if (type.equals(SMALLINT)) {
            SMALLINT.writeShort(blockBuilder, value == null ? 0 : (short) value);
        }
        else if (type.equals(TINYINT)) {
            TINYINT.writeByte(blockBuilder, value == null ? 0 : (byte) value);
        }
        else if (type.equals(REAL)) {
            REAL.writeFloat(blockBuilder, value == null ? 0.0F : (float) value);
        }
        else if (type.equals(DOUBLE)) {
            DOUBLE.writeDouble(blockBuilder, value == null ? 0.0 : (double) value);
        }
        else if (type instanceof VarcharType varcharType) {
            varcharType.writeSlice(blockBuilder, varcharType.getSlice(block, position));
        }
        else if (type instanceof CharType charType) {
            charType.writeSlice(blockBuilder, padSpaces(charType.getSlice(block, position), charType));
        }
        else if (type.equals(VARBINARY)) {
            VARBINARY.writeSlice(blockBuilder, VARBINARY.getSlice(block, position));
        }
        else if (type.equals(DATE)) {
            DATE.writeLong(blockBuilder, DATE.getLong(block, position));
        }
        else if (type.equals(TIME_MILLIS)) {
            TIME_MILLIS.writeLong(blockBuilder, TIME_MILLIS.getLong(block, position));
        }
        else if (type.equals(TIMESTAMP_MILLIS)) {
            TIMESTAMP_MILLIS.writeLong(blockBuilder, TIMESTAMP_MILLIS.getLong(block, position));
        }
        else if (type.equals(TIMESTAMP_TZ_MILLIS)) {
            TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, TIMESTAMP_TZ_MILLIS.getLong(block, position));
        }
        else if (type instanceof DecimalType decimalType) {
            decimalType.writeSlice(blockBuilder, decimalType.getSlice(block, position));
        }
//        else if (type instanceof ArrayType arrayType) {
//            Block arrayBlock = arrayType.getObject(block, position);
//            BlockBuilder arrayBlockBuilder = blockBuilder.beginBlockEntry();
//            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
//                getValueBlock(arrayType.getElementType(), arrayBlock, arrayBlockBuilder, i);
//            }
//            blockBuilder.closeEntry();
//        } else if (type instanceof MapType mapType) {
//            SqlMap sqlMap = mapType.getObject(block, position);
//            BlockBuilder mapBlockBuilder = blockBuilder.beginBlockEntry();
//            for (int i = 0; i < sqlMap.getSize(); i++) {
//                getValueBlock(mapType.getKeyType(), sqlMap.getRawKeyBlock(), mapBlockBuilder, sqlMap.getRawOffset() + i);
//                getValueBlock(mapType.getValueType(), sqlMap.getRawValueBlock(), mapBlockBuilder, sqlMap.getRawOffset() + i);
//            }
//            blockBuilder.closeEntry();
//        } else if (type instanceof RowType rowType) {
//            SqlRow sqlRow = rowType.getObject(block, position);
//            BlockBuilder rowBlockBuilder = blockBuilder.beginBlockEntry();
//            for (int i = 0; i < rowType.getTypeParameters().size(); i++) {
//                getValueBlock(rowType.getTypeParameters().get(i), sqlRow.getRawFieldBlock(i), rowBlockBuilder, sqlRow.getRawIndex());
//            }
//            blockBuilder.closeEntry();
//        }
        else {
            throw new TrinoException(NOT_SUPPORTED, "unsupported type: " + type);
        }
    }

    private Object getObjectValue(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (type.equals(BOOLEAN)) {
            return BOOLEAN.getBoolean(block, position);
        }
        if (type.equals(BIGINT)) {
            return BIGINT.getLong(block, position);
        }
        if (type.equals(INTEGER)) {
            return INTEGER.getInt(block, position);
        }
        if (type.equals(SMALLINT)) {
            return SMALLINT.getShort(block, position);
        }
        if (type.equals(TINYINT)) {
            return TINYINT.getByte(block, position);
        }
        if (type.equals(REAL)) {
            return REAL.getFloat(block, position);
        }
        if (type.equals(DOUBLE)) {
            return DOUBLE.getDouble(block, position);
        }
        if (type instanceof VarcharType varcharType) {
            return varcharType.getSlice(block, position).toStringUtf8();
        }
        if (type instanceof CharType charType) {
            return padSpaces(charType.getSlice(block, position), charType).toStringUtf8();
        }
        if (type.equals(VARBINARY)) {
            return VARBINARY.getSlice(block, position).getBytes();
        }
        if (type.equals(DATE)) {
            int days = DATE.getInt(block, position);
            return LocalDate.ofEpochDay(days);
        }
        if (type.equals(TIME_MILLIS)) {
            long picos = TIME_MILLIS.getLong(block, position);
            return LocalTime.ofNanoOfDay(roundDiv(picos, PICOSECONDS_PER_NANOSECOND));
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            long millisUtc = floorDiv(TIMESTAMP_MILLIS.getLong(block, position), MICROSECONDS_PER_MILLISECOND);
            Instant instant = Instant.ofEpochMilli(millisUtc);
            return LocalDateTime.ofInstant(instant, UTC);
        }
        if (type.equals(TIMESTAMP_TZ_MILLIS)) {
            long millisUtc = unpackMillisUtc(TIMESTAMP_TZ_MILLIS.getLong(block, position));
            Instant instant = Instant.ofEpochMilli(millisUtc);
            return LocalDateTime.ofInstant(instant, UTC);
        }
        if (type instanceof DecimalType decimalType) {
            return readBigDecimal(decimalType, block, position);
        }
        if (type instanceof ArrayType arrayType) {
            Type elementType = arrayType.getElementType();

            Block arrayBlock = arrayType.getObject(block, position);

            List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                Object element = getObjectValue(elementType, arrayBlock, i);
                list.add(element);
            }

            return unmodifiableList(list);
        }
        if (type instanceof MapType mapType) {
            Type keyType = mapType.getKeyType();
            Type valueType = mapType.getValueType();

            SqlMap sqlMap = mapType.getObject(block, position);
            int size = sqlMap.getSize();
            int rawOffset = sqlMap.getRawOffset();
            Block rawKeyBlock = sqlMap.getRawKeyBlock();
            Block rawValueBlock = sqlMap.getRawValueBlock();

            // map type is converted into list of fixed keys document
            List<Object> values = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                Map<String, Object> mapValue = new HashMap<>();
                mapValue.put("key", getObjectValue(keyType, rawKeyBlock, rawOffset + i));
                mapValue.put("value", getObjectValue(valueType, rawValueBlock, rawOffset + i));
                values.add(mapValue);
            }

            return unmodifiableList(values);
        }
        if (type instanceof RowType rowType) {
            SqlRow sqlRow = rowType.getObject(block, position);
            int rawIndex = sqlRow.getRawIndex();

            List<Type> fieldTypes = rowType.getTypeParameters();
            if (fieldTypes.size() != sqlRow.getFieldCount()) {
                throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Expected row value field count does not match type field count");
            }
            Map<String, Object> rowValue = new HashMap<>();
            for (int i = 0; i < sqlRow.getFieldCount(); i++) {
                rowValue.put(
                        rowType.getTypeSignature().getParameters().get(i).getNamedTypeSignature().getName().orElse("field" + i),
                        getObjectValue(fieldTypes.get(i), sqlRow.getRawFieldBlock(i), rawIndex));
            }
            return unmodifiableMap(rowValue);
        }

        throw new TrinoException(NOT_SUPPORTED, "unsupported type: " + type);
    }
}
