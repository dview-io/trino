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
package io.trino.plugin.dview.split;

import com.google.inject.Inject;
import io.dview.schema.fortress.models.schema.file.Segment;
import io.trino.plugin.dview.client.DviewClient;
import io.trino.plugin.dview.table.DviewTableHandle;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.stream.Collectors;

public class DviewSplitManager
            implements ConnectorSplitManager
{
    private final DviewClient client;
    private final NodeManager nodeManager;

    @Inject
    public DviewSplitManager(DviewClient client, NodeManager nodeManager)
    {
        this.client = client;
        this.nodeManager = nodeManager;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        DviewTableHandle dviewTableHandle = (DviewTableHandle) connectorTableHandle;
        List<HostAddress> addresses = nodeManager.getRequiredWorkerNodes().stream().map(Node::getHostAndPort).toList();
        List<Segment> segments = client.getDocumentContract().getSegments(dviewTableHandle.getEntity());
        List<DviewSplit> splits = segments.stream()
                .flatMap((segment -> segment.getDocuments().stream().map(document ->
                        new DviewSplit(addresses, document.getPath(), segment.getDateValue().toString(), segment.getTimeValue() != null ? segment.getTimeValue().toString() : null)
                ).toList().stream()))
                .collect(Collectors.toList());
        return new FixedSplitSource(splits);
    }
}
