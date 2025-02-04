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

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class SftpConfig
{
    private String connectorId;
    private String host;
    private int port;
    private String username;
    private String password;
    private int maxRetries;
    private int connectionTimeout;
    private String jdbcUrl;
    private String mysqlPassword;
    private String mysqlUsername;

    @NotNull
    public String getConnectorId()
    {
        return connectorId;
    }

    @NotNull
    public String getHost()
    {
        return host;
    }

    @NotNull
    public int getPort()
    {
        return port;
    }

    @NotNull
    public String getUsername()
    {
        return username;
    }

    @NotNull
    public String getPassword()
    {
        return password;
    }

    @NotNull
    public int getMaxRetries()
    {
        return maxRetries;
    }

    @NotNull
    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    @NotNull
    public String getMysqlPassword()
    {
        return mysqlPassword;
    }

    @NotNull
    public String getMysqlUsername()
    {
        return mysqlUsername;
    }

    @NotNull
    public int getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("connector.id")
    public SftpConfig setConnectorId(String connectorId)
    {
        this.connectorId = connectorId;
        return this;
    }

    @Config("sftp.host")
    public SftpConfig setHost(String host)
    {
        this.host = host;
        return this;
    }

    @Config("sftp.port")
    public SftpConfig setPort(int port)
    {
        this.port = port;
        return this;
    }

    @Config("sftp.username")
    public SftpConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @Config("sftp.password")
    public SftpConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @Config("sftp.max-retries")
    public SftpConfig setMaxRetries(int maxRetries)
    {
        this.maxRetries = maxRetries;
        return this;
    }

    @Config("sftp.connection-timeout")
    public SftpConfig setConnectionTimeout(int connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    @Config("mysql.jdbc-url")
    public SftpConfig setJdbcUrl(String jdbcUrl)
    {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    @Config("mysql.username")
    public SftpConfig setMysqlUsername(String mysqlUsername)
    {
        this.mysqlUsername = mysqlUsername;
        return this;
    }

    @Config("mysql.password")
    public SftpConfig setMysqlPassword(String mysqlPassword)
    {
        this.mysqlPassword = mysqlPassword;
        return this;
    }
}
