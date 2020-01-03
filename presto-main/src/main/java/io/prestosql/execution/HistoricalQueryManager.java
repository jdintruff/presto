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
package io.prestosql.execution;

import io.airlift.log.Logger;
import io.prestosql.spi.history.QueryHistorySource;
import io.prestosql.spi.history.QueryHistorySourceFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.prestosql.util.PropertiesUtil.loadProperties;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HistoricalQueryManager
{
    private static final Logger log = Logger.get(HistoricalQueryManager.class);
    private static final File HISTORY_CONFIGURATION = new File("etc/history.properties");
    private static final String HISTORY_MANAGER_PROPERTY_NAME = "history-manager.instance.names";
    private static final String HISTORY_MANAGER_CONF_DIR = "etc/history/";
    private static final String HISTORY_MANAGER_FACTORY_NAME = "history-manager.factory.name";

    private final Map<String, QueryHistorySourceFactory> historySourceFactories = new ConcurrentHashMap<>();
    private final List<QueryHistorySource> configuredHistorySources = Collections.synchronizedList(new ArrayList<>());

    public void addQueryHistorySourceFactory(QueryHistorySourceFactory queryHistorySourceFactory)
    {
        requireNonNull(queryHistorySourceFactory, "queryHistoryFactorySource is null");

        if (historySourceFactories.putIfAbsent(queryHistorySourceFactory.getName(), queryHistorySourceFactory) != null) {
            throw new IllegalArgumentException(format("Event listener '%s' is already registered", queryHistorySourceFactory.getName()));
        }
    }

    public void loadConfiguredQueryHistorySources()
            throws Exception
    {
        if (!HISTORY_CONFIGURATION.exists()) {
            log.warn("Unable to find query history manager configuration file " + HISTORY_CONFIGURATION);
        }
        else {
            Map<String, String> properties = new HashMap<>(loadProperties(HISTORY_CONFIGURATION));
            String queryHistoryInstanceNamesString = properties.remove(HISTORY_MANAGER_PROPERTY_NAME);
            checkArgument(!isNullOrEmpty(queryHistoryInstanceNamesString),
                    "History configuration %s does not contain %s", HISTORY_CONFIGURATION.getAbsoluteFile(), HISTORY_MANAGER_PROPERTY_NAME);
            for (String historySourceInstance : queryHistoryInstanceNamesString.split(",")) {
                File instanceConfFile = new File(HISTORY_MANAGER_CONF_DIR + historySourceInstance + ".properties");
                if (!instanceConfFile.exists()) {
                    continue;
                }
                Map<String, String> instanceProperties = new HashMap<>(loadProperties(instanceConfFile));
                checkArgument(!isNullOrEmpty(queryHistoryInstanceNamesString),
                        "History configuration %s does not contain %s", instanceConfFile.getAbsolutePath(), HISTORY_MANAGER_PROPERTY_NAME);
                String instanceFactoryName = instanceProperties.get(HISTORY_MANAGER_FACTORY_NAME);
                if (historySourceFactories.get(instanceFactoryName) != null) {
                    configuredHistorySources.add(historySourceFactories.get(instanceFactoryName).create(instanceProperties));
                }
            }
        }
    }

    public QueryInfo getQueryById(String queryId)
    {
        for (QueryHistorySource source : configuredHistorySources) {
            QueryInfo info = (QueryInfo) source.getQueryById(queryId);
            if (info != null) {
                return info;
            }
        }
        return null;
    }
}
