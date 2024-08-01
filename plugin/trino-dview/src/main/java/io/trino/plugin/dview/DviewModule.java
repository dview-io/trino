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
package io.trino.plugin.dview;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.ConfigBinder;
import io.trino.plugin.dview.client.DviewClient;
import io.trino.plugin.dview.connector.DviewConnector;
import io.trino.plugin.dview.connector.DviewConnectorMetadata;
import io.trino.plugin.dview.page.DviewPageSinkProvider;
import io.trino.plugin.dview.page.DviewPageSourceProvider;
import io.trino.plugin.dview.split.DviewSplitManager;
import io.trino.plugin.dview.utils.DviewSessionProperties;
import io.trino.plugin.dview.utils.DviewTableProperties;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
//import io.trino.plugin.hive.orc.OrcReaderConfig;
//import io.trino.plugin.hive.parquet.ParquetReaderConfig;

//import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;
//import io.trino.plugin.dview.record.DviewSimpleParquetRecordProvider;
//import io.trino.plugin.dview.split.DviewSplitManager;

public class DviewModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ParquetReaderConfig.class).in(Scopes.SINGLETON);
        binder.bind(ParquetWriterConfig.class).in(Scopes.SINGLETON);
        binder.bind(DviewConnector.class).in(Scopes.SINGLETON);
        binder.bind(DviewConnectorMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DviewClient.class).in(Scopes.SINGLETON);
        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        binder.bind(DviewTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(DviewSessionProperties.class).in(Scopes.SINGLETON);
//        configBinder(binder).bindConfig(ParquetReaderConfig.class);
//        configBinder(binder).bindConfig(OrcReaderConfig.class);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();
        binder.bind(DviewSplitManager.class).in(Scopes.SINGLETON);
//        binder.bind(TrinoFileSystemFactory.class).in(Scopes.SINGLETON);
        binder.bind(DviewPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(DviewPageSinkProvider.class).in(Scopes.SINGLETON);
//        binder.bind(TestSimpleParquetRecordProvider.class).in(Scopes.SINGLETON);

        ConfigBinder.configBinder(binder).bindConfig(DviewConfig.class);
    }
}
