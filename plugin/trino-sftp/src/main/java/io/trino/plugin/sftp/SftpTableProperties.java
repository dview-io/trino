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

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Map;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

public class SftpTableProperties
{
    public static final String DIRECTORY_PATH_PROPERTY = "directory_path";
    public static final String COMPRESSION_TYPE_PROPERTY = "compression_type";
    public static final String DELIMITER_PROPERTY = "delimiter";
    public static final String SKIP_HEADER_PROPERTY = "skip_header";

    private SftpTableProperties()
    {
    }

    public static List<PropertyMetadata<?>> getTableProperties()
    {
        return ImmutableList.of(
                stringProperty(
                        DIRECTORY_PATH_PROPERTY,
                        "SFTP directory path",
                        null,
                        true),
                stringProperty(
                        COMPRESSION_TYPE_PROPERTY,
                        "Compression type (NONE, GZIP, SNAPPY, ZIP)",
                        "NONE",
                        false),
                stringProperty(
                        DELIMITER_PROPERTY,
                        "CSV delimiter character",
                        ",",
                        false),
                booleanProperty(
                        SKIP_HEADER_PROPERTY,
                        "Skip header row",
                        false,
                        false));
    }

    public static String getDirectoryPath(Map<String, Object> properties)
    {
        return (String) requireNonNull(properties.get(DIRECTORY_PATH_PROPERTY),
                "directory-path property must be specified");
    }

    public static String getCompressionType(Map<String, Object> properties)
    {
        return (String) properties.getOrDefault(COMPRESSION_TYPE_PROPERTY, "NONE");
    }

    public static String getDelimiter(Map<String, Object> properties)
    {
        return (String) properties.getOrDefault(DELIMITER_PROPERTY, ",");
    }

    public static boolean isSkipHeader(Map<String, Object> properties)
    {
        return (Boolean) properties.getOrDefault(SKIP_HEADER_PROPERTY, false);
    }
}
