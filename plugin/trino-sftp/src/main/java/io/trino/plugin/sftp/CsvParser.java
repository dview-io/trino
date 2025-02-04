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

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

public class CsvParser
{
    private static final DateTimeFormatter[] TIMESTAMP_FORMATTERS = {
            DateTimeFormatter.ISO_LOCAL_DATE_TIME,
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
    };
    private final List<ColumnMetadata> columns;
    private final boolean skipHeader;
    private final char delimiter;

    public CsvParser(List<ColumnMetadata> columns, boolean skipHeader, char delimiter)
    {
        this.columns = columns;
        this.skipHeader = skipHeader;
        this.delimiter = delimiter;
    }

    public List<List<Object>> parse(InputStream inputStream)
    {
        List<List<Object>> records = new ArrayList<>();
        int lineNumber = 0;

        try (CSVReader reader = new CSVReader(new InputStreamReader(inputStream))) {
            String[] nextLine;

            // Skip header if required
            if (skipHeader) {
                String[] headerLine = reader.readNext();
                if (headerLine != null) {
                    lineNumber++;
                }
            }

            while ((nextLine = reader.readNext()) != null) {
                lineNumber++;
                try {
                    List<Object> record = parseRecord(nextLine);
                    records.add(record);
                }
                catch (Exception e) {
                    // Log warning and continue
                    System.err.printf("Warning: Error parsing line %d: %s%n", lineNumber, e.getMessage());
                }
            }
        }
        catch (IOException | CsvValidationException e) {
            throw new RuntimeException("Failed to parse CSV file", e);
        }

        return records;
    }

    private List<Object> parseRecord(String[] values)
    {
        List<Object> record = new ArrayList<>();

        for (int i = 0; i < columns.size(); i++) {
            String value = i < values.length ? values[i].trim() : "";
            Type type = columns.get(i).getType();

            try {
                record.add(parseValue(value, type));
            }
            catch (Exception e) {
                // Add null for invalid values
                record.add(null);
            }
        }

        return record;
    }

    private Object parseValue(String value, Type type)
    {
        if (value.isEmpty()) {
            return null;
        }
        if (type instanceof TimestampType) {
            return parseTimestamp(value.trim());
        }
        try {
            return switch (type.getDisplayName()) {
                case "integer" -> Integer.parseInt(value);
                case "bigint" -> Long.parseLong(value);
                case "double" -> Double.parseDouble(value);
                case "boolean" -> Boolean.parseBoolean(value);
                case "date" -> LocalDate.parse(value.trim()).toEpochDay();
                case "varchar", "text" -> value;
                default -> throw new IllegalArgumentException("Unsupported type: " + type);
            };
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for type " + type + ": " + value);
        }
    }

    private Long parseTimestamp(String value)
    {
        for (DateTimeFormatter formatter : TIMESTAMP_FORMATTERS) {
            try {
                LocalDateTime dateTime = LocalDateTime.parse(value, formatter);
                return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
            }
            catch (DateTimeParseException e) {
                // Try next formatter
                continue;
            }
        }
        throw new IllegalArgumentException("Cannot parse timestamp: " + value);
    }
}
