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

import io.airlift.configuration.Config;
import io.dview.schema.fortress.client.constants.ClientType;
import io.dview.schema.fortress.client.constants.FortressClientConfig;

import javax.validation.constraints.NotNull;

import java.util.Locale;
import java.util.Map;

public class DviewConfig
{
    private String mysqlUrl;
    private String mysqlUsername;
    private String mysqlPassword;
    private int connectionTimeout;
    private Map<String, Object> mysqlAdditionalProperties;
    private ClientType clientType;
    private String tenant;
    private String namespace;
    private String org;
    String accessKey;
    String secretKey;
    String region;

    @NotNull
    public String getMysqlUrl()
    {
        return mysqlUrl;
    }

    @NotNull
    public String getMysqlUsername()
    {
        return mysqlUsername;
    }

    @NotNull
    public String getMysqlPassword()
    {
        return mysqlPassword;
    }

    @NotNull
    public String getNamespace()
    {
        return namespace;
    }

    @NotNull
    public int getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @NotNull
    public ClientType getClientType()
    {
        return clientType;
    }

    @NotNull
    public String getOrg()
    {
        return org;
    }

    @NotNull
    public String getTenant()
    {
        return tenant;
    }

    @NotNull
    public String getAccessKey()
    { return accessKey; }

    @NotNull
    public String getRegion()
    {
        return region;
    }

    @NotNull
    public String getSecretKey()
    {
        return secretKey;
    }

    @Config("mysql.url")
    public DviewConfig setMysqlUrl(String mysqlUrl)
    {
        this.mysqlUrl = mysqlUrl;
        return this;
    }

    @Config("mysql.username")
    public DviewConfig setMysqlUsername(String mysqlUsername)
    {
        this.mysqlUsername = mysqlUsername;
        return this;
    }

    @Config("mysql.password")
    public DviewConfig setMysqlPassword(String mysqlPassword)
    {
        this.mysqlPassword = mysqlPassword;
        return this;
    }

    @Config("connection.timeout")
    public DviewConfig setConnectionTimeout(int connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    @Config("client.type")
    public DviewConfig setClientType(String clientType)
    {
        this.clientType = ClientType.valueOf(clientType.toUpperCase(Locale.ROOT));
        return this;
    }

    @Config("dview.tenant")
    public DviewConfig setTenant(String tenant)
    {
        this.tenant = tenant;
        return this;
    }

    @Config("dview.namespace")
    public DviewConfig setNamespace(String namespace)
    {
        this.namespace = namespace;
        return this;
    }

    @Config("dview.org")
    public DviewConfig setOrg(String org)
    {
        this.org = org;
        return this;
    }

    @Config("s3.accessKey")
    public DviewConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    @Config("s3.secretKey")
    public DviewConfig setSecretKey(String secretKey)
    {
        this.secretKey = secretKey;
        return this;
    }

    @Config("s3.region")
    public DviewConfig setRegion(String region)
    {
        this.region = region;
        return this;
    }

    public FortressClientConfig getFortressClientConfig()
    {
        return FortressClientConfig.builder()
                .clientType(getClientType())
                .mysqlUsername(getMysqlUsername())
                .mysqlUrl(getMysqlUrl())
                .mysqlPassword(getMysqlPassword())
                .connectionTimeout(getConnectionTimeout())
                .mysqlAdditionalProperties(null)
                .accessKey(getAccessKey())
                .secretKey(getSecretKey())
                .region(getRegion())
                .build();
    }
}
