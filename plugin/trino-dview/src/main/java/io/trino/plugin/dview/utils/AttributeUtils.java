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

import io.dview.schema.fortress.models.schema.entity.properties.AttributeType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

public class AttributeUtils
{
    private AttributeUtils()
    {
    }

    public static Type convertFortressTypeToTrinoType(AttributeType.Type type)
    {
        switch (type) {
            case BOOLEAN:
            case TINYINT:
                return BooleanType.BOOLEAN;
            case SMALLINT:
            case INT:
                return IntegerType.INTEGER;
            case BIGINT:
                return BigintType.BIGINT;
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return DoubleType.DOUBLE;
            case DATE:
                return DateType.DATE;
            case DATETIME:
            case TIMESTAMP:
                return TimestampType.TIMESTAMP_MILLIS;
            case TIME:
                return TimeType.TIME_MILLIS;
            case CHAR:
                return CharType.createCharType(1);
            case JSON:
            case BINARY:
            case BLOB:
                return VarbinaryType.VARBINARY;
            case TEXT:
            case VARCHAR:
            default:
                return VarcharType.VARCHAR;
        }
    }
}
