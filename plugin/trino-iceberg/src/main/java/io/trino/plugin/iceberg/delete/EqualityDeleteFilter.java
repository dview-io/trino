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
package io.trino.plugin.iceberg.delete;

import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.StructProjection;

import java.util.List;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.iceberg.IcebergUtil.schemaFromHandles;
import static java.util.Objects.requireNonNull;

public final class EqualityDeleteFilter
        implements DeleteFilter
{
    private final Schema deleteSchema;
    private final StructLikeSet deletedRows;

    private EqualityDeleteFilter(Schema deleteSchema, StructLikeSet deletedRows)
    {
        this.deleteSchema = requireNonNull(deleteSchema, "deleteSchema is null");
        this.deletedRows = requireNonNull(deletedRows, "deletedRows is null");
    }

    @Override
    public RowPredicate createPredicate(List<IcebergColumnHandle> columns)
    {
        Schema fileSchema = schemaFromHandles(columns);
        if (deleteSchema.columns().stream().anyMatch(column -> fileSchema.findField(column.fieldId()) == null)) {
            throw new TrinoException(ICEBERG_CANNOT_OPEN_SPLIT, "columns list doesn't contain all equality delete columns");
        }

        StructProjection projection = StructProjection.create(fileSchema, deleteSchema);
        Type[] types = columns.stream()
                .map(IcebergColumnHandle::getType)
                .toArray(Type[]::new);

        return (page, position) -> {
            StructProjection row = projection.wrap(new LazyTrinoRow(types, page, position));
            return deletedRows.contains(row);
        };
    }

    public static EqualityDeleteFilterBuilder builder(Schema deleteSchema)
    {
        return new EqualityDeleteFilterBuilder(deleteSchema);
    }

    public static class EqualityDeleteFilterBuilder
    {
        private final Schema deleteSchema;
        private final StructLikeSet deletedRows;

        private EqualityDeleteFilterBuilder(Schema deleteSchema)
        {
            this.deleteSchema = requireNonNull(deleteSchema, "deleteSchema is null");
            this.deletedRows = StructLikeSet.create(deleteSchema.asStruct());
        }

        public void readEqualityDeletes(ConnectorPageSource pageSource, List<IcebergColumnHandle> columns)
        {
            Type[] types = columns.stream()
                    .map(IcebergColumnHandle::getType)
                    .toArray(Type[]::new);

            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page == null) {
                    continue;
                }

                for (int position = 0; position < page.getPositionCount(); position++) {
                    deletedRows.add(new TrinoRow(types, page, position));
                }
            }
        }

        public EqualityDeleteFilter build()
        {
            return new EqualityDeleteFilter(deleteSchema, deletedRows);
        }
    }
}
