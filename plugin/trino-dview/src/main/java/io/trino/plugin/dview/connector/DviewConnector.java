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
package io.trino.plugin.dview.connector;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.dview.DviewTransactionHandle;
import io.trino.plugin.dview.page.DviewPageSinkProvider;
import io.trino.plugin.dview.page.DviewPageSourceProvider;
import io.trino.plugin.dview.split.DviewSplitManager;
import io.trino.plugin.dview.utils.DviewTableProperties;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class DviewConnector
        implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final DviewConnectorMetadata metadata;
    private final DviewPageSourceProvider dviewPageSourceProvider;
    private final DviewSplitManager dviewSplitManager;
    private final DviewTableProperties dviewTableProperties;
    private final DviewPageSinkProvider dviewPageSinkProvider;

    @Inject
    public DviewConnector(LifeCycleManager lifeCycleManager, DviewConnectorMetadata metadata, DviewSplitManager dviewSplitManager, DviewPageSourceProvider dviewPageSourceProvider, DviewTableProperties dviewTableProperties, DviewPageSinkProvider dviewPageSinkProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "DviewConnectorMetadata is null");
        this.dviewSplitManager = requireNonNull(dviewSplitManager, "dviewSplitManager is null");
        this.dviewPageSourceProvider = requireNonNull(dviewPageSourceProvider, "dviewPageSourceProvider is null");
        this.dviewTableProperties = requireNonNull(dviewTableProperties, "dviewTableProperties is null");
        this.dviewPageSinkProvider = requireNonNull(dviewPageSinkProvider, "dviewPageSinkProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return DviewTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return dviewSplitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return this.dviewPageSourceProvider;
    }

    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return dviewTableProperties.getTableProperties();
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return this.dviewPageSinkProvider;
    }
}
