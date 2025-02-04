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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class SftpConnectorFactory
        implements ConnectorFactory
{
    private static final String CONNECTOR_NAME = "sftp";

    @Override
    public String getName()
    {
        return CONNECTOR_NAME;
    }

    @Override
    public Connector create(String catalogName,
                            Map<String, String> config,
                            ConnectorContext context)
    {
        requireNonNull(config, "config is null");
        Bootstrap app = new Bootstrap(
                binder -> binder.bind(NodeManager.class).toInstance(context.getNodeManager()),
                new JsonModule(),
                new TypeDeserializerModule(context.getTypeManager()),
                new SftpModule());
        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(SftpConnector.class);
    }
}
