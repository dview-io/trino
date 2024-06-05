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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.TupleDomain;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.cloud.bigquery.TableDefinition.Type.EXTERNAL;
import static com.google.cloud.bigquery.TableDefinition.Type.MATERIALIZED_VIEW;
import static com.google.cloud.bigquery.TableDefinition.Type.VIEW;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.bigquery.BigQueryClient.TABLE_TYPES;
import static io.trino.plugin.bigquery.BigQueryClient.selectSql;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_FAILED_TO_EXECUTE_QUERY;
import static io.trino.plugin.bigquery.BigQuerySessionProperties.isSkipViewMaterialization;
import static io.trino.plugin.bigquery.BigQueryUtil.isWildcardTable;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static org.apache.arrow.vector.ipc.message.MessageSerializer.deserializeSchema;

public class BigQuerySplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(BigQuerySplitManager.class);

    private final BigQueryClientFactory bigQueryClientFactory;
    private final BigQueryReadClientFactory bigQueryReadClientFactory;
    private final Optional<Integer> parallelism;
    private final boolean viewEnabled;
    private final boolean arrowSerializationEnabled;
    private final Duration viewExpiration;
    private final NodeManager nodeManager;
    private final int maxReadRowsRetries;

    @Inject
    public BigQuerySplitManager(
            BigQueryConfig config,
            BigQueryClientFactory bigQueryClientFactory,
            BigQueryReadClientFactory bigQueryReadClientFactory,
            NodeManager nodeManager)
    {
        this.bigQueryClientFactory = requireNonNull(bigQueryClientFactory, "bigQueryClientFactory cannot be null");
        this.bigQueryReadClientFactory = requireNonNull(bigQueryReadClientFactory, "bigQueryReadClientFactory cannot be null");
        this.parallelism = config.getParallelism();
        this.viewEnabled = config.isViewsEnabled();
        this.arrowSerializationEnabled = config.isArrowSerializationEnabled();
        this.viewExpiration = config.getViewExpireDuration();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager cannot be null");
        this.maxReadRowsRetries = config.getMaxReadRowsRetries();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        log.debug("getSplits(transaction=%s, session=%s, table=%s)", transaction, session, table);
        BigQueryTableHandle bigQueryTableHandle = (BigQueryTableHandle) table;

        int actualParallelism = parallelism.orElseGet(() -> nodeManager.getRequiredWorkerNodes().size());
        TupleDomain<ColumnHandle> tableConstraint = bigQueryTableHandle.constraint();
        Optional<String> filter = BigQueryFilterQueryBuilder.buildFilter(tableConstraint);

        if (!bigQueryTableHandle.isNamedRelation()) {
            List<BigQueryColumnHandle> columns = bigQueryTableHandle.projectedColumns().orElse(ImmutableList.of());
            return new FixedSplitSource(BigQuerySplit.forViewStream(columns, filter));
        }

        TableId remoteTableId = bigQueryTableHandle.asPlainTable().getRemoteTableName().toTableId();
        TableDefinition.Type tableType = TableDefinition.Type.valueOf(bigQueryTableHandle.asPlainTable().getType());
        List<BigQuerySplit> splits = emptyProjectionIsRequired(bigQueryTableHandle.projectedColumns())
                ? createEmptyProjection(session, tableType, remoteTableId, filter)
                : readFromBigQuery(session, tableType, remoteTableId, bigQueryTableHandle.projectedColumns(), actualParallelism, tableConstraint);
        return new FixedSplitSource(splits);
    }

    private static boolean emptyProjectionIsRequired(Optional<List<BigQueryColumnHandle>> projectedColumns)
    {
        return projectedColumns.isPresent() && projectedColumns.get().isEmpty();
    }

    private List<BigQuerySplit> readFromBigQuery(ConnectorSession session, TableDefinition.Type type, TableId remoteTableId, Optional<List<BigQueryColumnHandle>> projectedColumns, int actualParallelism, TupleDomain<ColumnHandle> tableConstraint)
    {
        checkArgument(projectedColumns.isPresent() && projectedColumns.get().size() > 0, "Projected column is empty");
        Optional<String> filter = BigQueryFilterQueryBuilder.buildFilter(tableConstraint);

        log.debug("readFromBigQuery(tableId=%s, projectedColumns=%s, actualParallelism=%s, filter=[%s])", remoteTableId, projectedColumns, actualParallelism, filter);
        List<BigQueryColumnHandle> columns = projectedColumns.get();
        List<String> projectedColumnsNames = new ArrayList<>(columns.stream().map(BigQueryColumnHandle::name).toList());

        if (isWildcardTable(type, remoteTableId.getTable())) {
            // Storage API doesn't support reading wildcard tables
            return ImmutableList.of(BigQuerySplit.forViewStream(columns, filter));
        }
        if (type == EXTERNAL) {
            // Storage API doesn't support reading external tables
            return ImmutableList.of(BigQuerySplit.forViewStream(columns, filter));
        }
        if (type == VIEW || type == MATERIALIZED_VIEW) {
            if (isSkipViewMaterialization(session)) {
                return ImmutableList.of(BigQuerySplit.forViewStream(columns, filter));
            }
            tableConstraint.getDomains().ifPresent(domains -> domains.keySet().stream()
                    .map(column -> ((BigQueryColumnHandle) column).name())
                    .filter(columnName -> !projectedColumnsNames.contains(columnName))
                    .forEach(projectedColumnsNames::add));
        }
        ReadSession readSession = createReadSession(session, remoteTableId, ImmutableList.copyOf(projectedColumnsNames), filter, actualParallelism);

        String schemaString = getSchemaAsString(readSession);
        return readSession.getStreamsList().stream()
                .map(stream -> BigQuerySplit.forStream(stream.getName(), schemaString, columns, OptionalInt.of(stream.getSerializedSize())))
                .collect(toImmutableList());
    }

    @VisibleForTesting
    ReadSession createReadSession(ConnectorSession session, TableId remoteTableId, List<String> projectedColumnsNames, Optional<String> filter, int actualParallelism)
    {
        ReadSessionCreator readSessionCreator = new ReadSessionCreator(bigQueryClientFactory, bigQueryReadClientFactory, viewEnabled, arrowSerializationEnabled, viewExpiration, maxReadRowsRetries);
        return readSessionCreator.create(session, remoteTableId, projectedColumnsNames, filter, actualParallelism);
    }

    private List<BigQuerySplit> createEmptyProjection(ConnectorSession session, TableDefinition.Type tableType, TableId remoteTableId, Optional<String> filter)
    {
        if (!TABLE_TYPES.contains(tableType)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported table type: " + tableType);
        }

        BigQueryClient client = bigQueryClientFactory.create(session);
        log.debug("createEmptyProjection(tableId=%s, filter=[%s])", remoteTableId, filter);
        try {
            // Note that we cannot use row count from TableInfo because for writes via insertAll/streaming API the number is incorrect until the streaming buffer is flushed
            // (and there's no mechanism to trigger an on-demand flush). This can lead to incorrect results for queries with empty projections.
            String sql = selectSql(remoteTableId, "COUNT(*)", filter);
            TableResult result = client.executeQuery(session, sql);
            long numberOfRows = result.iterateAll().iterator().next().getFirst().getLongValue();

            return ImmutableList.of(BigQuerySplit.emptyProjection(numberOfRows));
        }
        catch (BigQueryException e) {
            throw new TrinoException(BIGQUERY_FAILED_TO_EXECUTE_QUERY, "Failed to compute empty projection", e);
        }
    }

    private String getSchemaAsString(ReadSession readSession)
    {
        if (arrowSerializationEnabled) {
            return deserializeArrowSchema(readSession.getArrowSchema().getSerializedSchema());
        }
        return readSession.getAvroSchema().getSchema();
    }

    private static String deserializeArrowSchema(ByteString serializedSchema)
    {
        try {
            return deserializeSchema(new ReadChannel(new ByteArrayReadableSeekableByteChannel(serializedSchema.toByteArray())))
                    .toJson();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
