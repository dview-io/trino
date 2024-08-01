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

import io.dview.schema.fortress.models.schema.meta.CloudProvider;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.s3.HiveS3Config;
import io.trino.hdfs.s3.TrinoS3ConfigurationInitializer;

public class FileUtils
{
    private FileUtils()
    {
    }

    public static ConfigurationInitializer getConfigurationInitializerForCloudProvider(String storagePath, CloudProvider cloudProvider)
    {
        switch (cloudProvider.getName()) {
            case S3:
                HiveS3Config hiveS3Config = new HiveS3Config();
                hiveS3Config.setS3AwsAccessKey(System.getenv("accessKey"));
                hiveS3Config.setS3AwsSecretKey(System.getenv("secretKey"));
                hiveS3Config.setS3Region(cloudProvider.getRegion());
//                hiveS3Config.setS3SslEnabled(true);
//                hiveS3Config.setS3PathStyleAccess(true);
                return new TrinoS3ConfigurationInitializer(hiveS3Config);
            default: return config -> cloudProvider.getConfigs().forEach((key, value) -> config.set(key, extractConfigValue(value.toString())));
        }
    }

    /**
     * Extracts and resolves the value of a configuration string that may contain references to
     * environment variables. If the input string starts with "${" and ends with "}", it is considered
     * a reference to an environment variable. The method attempts to fetch the environment variable's
     * value and replace the input string with that value. If the environment variable is not found,
     * it throws a {@link RuntimeException} with an error message indicating the missing variable.
     *
     * @param configValue The configuration string that may contain references to environment variables.
     * @return The resolved configuration value, either the environment variable's value or the original input string.
     * @throws RuntimeException If the referenced environment variable is not found.
     */
    public static String extractConfigValue(String configValue)
    {
        if (configValue.startsWith("${") && configValue.endsWith("}")) {
            // Extract the environment variable name between "${" and "}"
            String envVarName = configValue.substring(2, configValue.length() - 1);

            // Fetch the environment variable value
            String envVarValue = System.getenv(envVarName);

            if (envVarValue != null) {
                // Replace the input with the environment variable value
                configValue = envVarValue;
            }
            else {
                // Handle the case where the environment variable is not found
                throw new RuntimeException("Environment variable not found: " + envVarName);
            }
        }
        return configValue;
    }
}
