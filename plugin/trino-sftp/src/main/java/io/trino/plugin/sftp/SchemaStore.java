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

import com.google.inject.Inject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;

public class SchemaStore
        implements AutoCloseable
{
    private static final Logger log = Logger.get(SchemaStore.class);
    private final DataSource dataSource;
    private final String connectorId;

    @Inject
    public SchemaStore(SftpConfig config)
    {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getJdbcUrl());
        hikariConfig.setUsername(config.getMysqlUsername());
        hikariConfig.setPassword(config.getMysqlPassword());
        hikariConfig.setMaximumPoolSize(10);
        this.dataSource = new HikariDataSource(hikariConfig);
        this.connectorId = config.getConnectorId();
        initializeTables();
    }

    private void initializeTables()
    {
        try (Connection conn = dataSource.getConnection()) {
            // Create schemas table first
            conn.createStatement().execute("""
                CREATE TABLE IF NOT EXISTS sftp_schemas (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    connector_id VARCHAR(255) NOT NULL,
                    schema_name VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_schema (connector_id, schema_name)
                )
            """);

            // Create tables table with proper schema_id reference
            conn.createStatement().execute("""
                CREATE TABLE IF NOT EXISTS sftp_tables (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    schema_id BIGINT NOT NULL,
                    table_name VARCHAR(255) NOT NULL,
                    directory_path TEXT NOT NULL,
                    compression_type VARCHAR(50) DEFAULT NULL,
                    delimiter CHAR(1) DEFAULT ',',
                    skip_header BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_table (schema_id, table_name),
                    FOREIGN KEY (schema_id) REFERENCES sftp_schemas(id)
                )
            """);

            // Create columns table
            conn.createStatement().execute("""
                CREATE TABLE IF NOT EXISTS sftp_columns (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    table_id BIGINT NOT NULL,
                    column_name VARCHAR(255) NOT NULL,
                    column_type VARCHAR(50) NOT NULL,
                    column_order INT NOT NULL,
                    FOREIGN KEY (table_id) REFERENCES sftp_tables(id),
                    UNIQUE KEY unique_column (table_id, column_name)
                )
            """);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to initialize schema tables", e);
        }
    }

    public Long getOrCreateSchema(String schemaName)
    {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // Try to find existing schema
                String selectSql = """
                    SELECT id FROM sftp_schemas
                    WHERE connector_id = ? AND schema_name = ?
                    """;
                try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                    ps.setString(1, connectorId);
                    ps.setString(2, schemaName);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            return rs.getLong("id");
                        }
                    }
                }

                // Create new schema if not exists
                String insertSql = """
                    INSERT INTO sftp_schemas (connector_id, schema_name)
                    VALUES (?, ?)
                    """;
                try (PreparedStatement ps = conn.prepareStatement(
                        insertSql, Statement.RETURN_GENERATED_KEYS)) {
                    ps.setString(1, connectorId);
                    ps.setString(2, schemaName);
                    ps.executeUpdate();
                    try (ResultSet rs = ps.getGeneratedKeys()) {
                        if (!rs.next()) {
                            throw new SQLException("Failed to get generated schema ID");
                        }
                        Long schemaId = rs.getLong(1);
                        conn.commit();
                        return schemaId;
                    }
                }
            }
            catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to create schema", e);
        }
    }

    public void createSchema(String schemaName)
    {
        try (Connection conn = dataSource.getConnection()) {
            // Check if schema exists
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT 1 FROM sftp_schemas WHERE connector_id = ? AND schema_name = ?")) {
                ps.setString(1, connectorId);
                ps.setString(2, schemaName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        throw new RuntimeException(schemaName + " already exists");
                    }
                }
            }

            // Create new schema
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO sftp_schemas (connector_id, schema_name) VALUES (?, ?)")) {
                ps.setString(1, connectorId);
                ps.setString(2, schemaName);
                ps.executeUpdate();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to create schema: " + schemaName, e);
        }
    }

    public void storeTableSchema(String schemaName, String tableName,
                                 List<ColumnMetadata> columns, Map<String, Object> properties)
    {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // Get or create schema ID
                Long schemaId = getOrCreateSchema(schemaName);

                // Insert table
                String insertTableSql = """
                    INSERT INTO sftp_tables (
                        schema_id, table_name, directory_path,
                        compression_type, delimiter, skip_header
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """;
                Long tableId;
                try (PreparedStatement ps = conn.prepareStatement(
                        insertTableSql, Statement.RETURN_GENERATED_KEYS)) {
                    ps.setLong(1, schemaId);
                    ps.setString(2, tableName);
                    ps.setString(3, SftpTableProperties.getDirectoryPath(properties));
                    ps.setString(4, SftpTableProperties.getCompressionType(properties));
                    ps.setString(5, SftpTableProperties.getDelimiter(properties));
                    ps.setBoolean(6, SftpTableProperties.isSkipHeader(properties));

                    ps.executeUpdate();
                    try (ResultSet rs = ps.getGeneratedKeys()) {
                        if (!rs.next()) {
                            throw new SQLException("Failed to get generated table ID");
                        }
                        tableId = rs.getLong(1);
                    }
                }

                // Insert columns
                String insertColumnSql = """
                    INSERT INTO sftp_columns (
                        table_id, column_name, column_type, column_order
                    ) VALUES (?, ?, ?, ?)
                    """;
                try (PreparedStatement ps = conn.prepareStatement(insertColumnSql)) {
                    for (int i = 0; i < columns.size(); i++) {
                        ColumnMetadata column = columns.get(i);
                        ps.setLong(1, tableId);
                        ps.setString(2, column.getName());
                        ps.setString(3, column.getType().toString());
                        ps.setInt(4, i);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }

                conn.commit();
            }
            catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to store table schema", e);
        }
    }


    public List<SchemaTableName> listTables(Optional<String> schemaNameParam)
    {
        String sql = """
            SELECT s.schema_name, t.table_name
            FROM sftp_tables t
            JOIN sftp_schemas s ON t.schema_id = s.id
            WHERE s.connector_id = ?
            """;

        if (schemaNameParam.isPresent()) {
            sql += " AND s.schema_name = ?";
        }

        List<SchemaTableName> tables = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, connectorId);
            if (schemaNameParam.isPresent()) {
                ps.setString(2, schemaNameParam.get());
            }

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tables.add(new SchemaTableName(
                            rs.getString("schema_name"),
                            rs.getString("table_name")
                    ));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to list tables", e);
        }
        return tables;
    }

    public TableMetadata getTableMetadata(SchemaTableName tableName)
    {
        try (Connection conn = dataSource.getConnection()) {
            String tableSql = """
            SELECT t.id as id, t.directory_path as directory_path, t.compression_type as compression_type, t.delimiter as delimiter, t.skip_header as skip_header
            FROM sftp_tables as t
            INNER JOIN sftp_schemas as s ON t.schema_id = s.id
            WHERE s.schema_name = ? AND table_name = ? AND connector_id = ?
            """;
            log.info("schema_name=%s, table_name=%s, connector_id=%s\n", tableName.getSchemaName(), tableName.getTableName(), connectorId);
            try (PreparedStatement ps = conn.prepareStatement(tableSql)) {
                ps.setString(1, tableName.getSchemaName());
                ps.setString(2, tableName.getTableName());
                ps.setString(3, connectorId);

                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new TableNotFoundException(tableName);
                    }

                    long tableId = rs.getLong("id");
                    String directoryPath = rs.getString("directory_path");
                    String compressionType = rs.getString("compression_type");
                    String delimiter = rs.getString("delimiter");
                    boolean skipHeader = rs.getBoolean("skip_header");

                    // Get column information
                    String columnSql = """
                    SELECT column_name, column_type, column_order
                    FROM sftp_columns
                    WHERE table_id = ?
                    ORDER BY column_order
                    """;

                    List<ColumnMetadata> columns = new ArrayList<>();
                    try (PreparedStatement colPs = conn.prepareStatement(columnSql)) {
                        colPs.setLong(1, tableId);
                        try (ResultSet colRs = colPs.executeQuery()) {
                            while (colRs.next()) {
                                String columnName = colRs.getString("column_name");
                                String columnType = colRs.getString("column_type");
                                Type type = parseTypeSignature(columnType);

                                columns.add(new ColumnMetadata(columnName, type));
                            }
                        }
                    }

                    return TableMetadata.builder()
                            .setSchemaTableName(tableName)
                            .setColumns(columns)
                            .setDirectoryPath(directoryPath)
                            .setCompressionType(compressionType)
                            .setDelimiter(delimiter)
                            .setSkipHeader(skipHeader)
                            .build();
                }
            }
        }
        catch (SQLException e) {
            e.printStackTrace(System.out);
            throw new RuntimeException("Failed to get table metadata", e);
        }
    }

    public void dropTable(SchemaTableName tableName)
    {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // First get the table ID
                String getIdSql = "SELECT id FROM sftp_tables WHERE schema_name = ? AND table_name = ? AND connector_id = ?";
                Long tableId;
                try (PreparedStatement ps = conn.prepareStatement(getIdSql)) {
                    ps.setString(1, tableName.getSchemaName());
                    ps.setString(2, tableName.getTableName());
                    ps.setString(3, connectorId);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) {
                            throw new TableNotFoundException(tableName);
                        }
                        tableId = rs.getLong("id");
                    }
                }

                // Delete columns first (due to foreign key constraint)
                String deleteColumnsSql = "DELETE FROM sftp_columns WHERE table_id = ?";
                try (PreparedStatement ps = conn.prepareStatement(deleteColumnsSql)) {
                    ps.setLong(1, tableId);
                    ps.executeUpdate();
                }

                // Then delete the table
                String deleteTableSql = "DELETE FROM sftp_tables WHERE id = ?";
                try (PreparedStatement ps = conn.prepareStatement(deleteTableSql)) {
                    ps.setLong(1, tableId);
                    ps.executeUpdate();
                }

                conn.commit();
            }
            catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to drop table", e);
        }
    }

    public void renameTable(SchemaTableName oldName, SchemaTableName newName)
    {
        try (Connection conn = dataSource.getConnection()) {
            // Check if target name already exists
            String checkSql = "SELECT 1 FROM sftp_tables WHERE schema_name = ? AND table_name = ? AND connector_id = ?";
            try (PreparedStatement ps = conn.prepareStatement(checkSql)) {
                ps.setString(1, newName.getSchemaName());
                ps.setString(2, newName.getTableName());
                ps.setString(3, connectorId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        throw new io.trino.spi.TrinoException(ALREADY_EXISTS,
                                format("Target table %s already exists", newName));
                    }
                }
            }

            // Perform the rename
            String renameSql = """
                    UPDATE sftp_tables
                    SET schema_name = ?, table_name = ?
                    WHERE schema_name = ? AND table_name = ? AND connector_id = ?
                    """;

            try (PreparedStatement ps = conn.prepareStatement(renameSql)) {
                ps.setString(1, newName.getSchemaName());
                ps.setString(2, newName.getTableName());
                ps.setString(3, oldName.getSchemaName());
                ps.setString(4, oldName.getTableName());
                ps.setString(5, connectorId);

                int updatedRows = ps.executeUpdate();
                if (updatedRows == 0) {
                    throw new TableNotFoundException(oldName);
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to rename table", e);
        }
    }

    public List<String> listSchemaNames()
    {
        List<String> schemas = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT schema_name FROM sftp_schemas WHERE connector_id = ? ORDER BY schema_name")) {
            ps.setString(1, connectorId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    schemas.add(rs.getString("schema_name"));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to list schemas", e);
        }
        return schemas;
    }

    public void dropSchema(String schemaName) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // Check if schema has tables
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT 1 FROM sftp_tables WHERE connector_id = ? AND schema_name = ?")) {
                    ps.setString(1, connectorId);
                    ps.setString(2, schemaName);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            throw new TrinoException(SCHEMA_NOT_EMPTY,
                                    "Schema " + schemaName + " is not empty");
                        }
                    }
                }

                // Delete schema
                try (PreparedStatement ps = conn.prepareStatement(
                        "DELETE FROM sftp_schemas WHERE connector_id = ? AND schema_name = ?")) {
                    ps.setString(1, connectorId);
                    ps.setString(2, schemaName);
                    int rowsDeleted = ps.executeUpdate();
                    if (rowsDeleted == 0) {
                        throw new SchemaNotFoundException(schemaName);
                    }
                }

                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to drop schema: " + schemaName, e);
        }
    }

    private Type parseTypeSignature(String type) {
        // Handle basic types
        return switch (type.toLowerCase(Locale.ENGLISH)) {
            case "boolean" -> BooleanType.BOOLEAN;
            case "tinyint" -> TinyintType.TINYINT;
            case "smallint" -> SmallintType.SMALLINT;
            case "integer" -> IntegerType.INTEGER;
            case "bigint" -> BigintType.BIGINT;
            case "real" -> RealType.REAL;
            case "double" -> DoubleType.DOUBLE;
            case "varchar" -> VarcharType.VARCHAR;
            case "date" -> DateType.DATE;
            case "timestamp", "timestamp(3)" -> TimestampType.TIMESTAMP_MILLIS;
            case "timestamp(6)" -> TimestampType.TIMESTAMP_MICROS;
            case "timestamp(9)" -> TimestampType.TIMESTAMP_NANOS;
            default -> throw new IllegalArgumentException("Unsupported type: " + type);
        };
    }

    // Helper method to format error messages
    private static String format(String message, Object... args)
    {
        return String.format(message, args);
    }

    public void validateTableLocation(String directoryPath)
    {
        // Check if the directory path is already in use
        String sql = "SELECT 1 FROM sftp_tables WHERE directory_path = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, directoryPath);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    throw new io.trino.spi.TrinoException(ALREADY_EXISTS,
                            format("Directory path %s is already in use", directoryPath));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to validate table location", e);
        }
    }

    @Override
    public void close()
    {
        if (dataSource instanceof HikariDataSource) {
            ((HikariDataSource) dataSource).close();
        }
    }
}
