/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.concurrent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogFile;

/**
 * Simulates an Oracle LogMiner session using an in-memory SQLite database.
 *
 * <p>Each log file (archive or redo) is represented as a separate SQLite table.
 * The {@link #query(List, Scn, Scn)} method constructs a CTE that unions the
 * selected log tables, then queries the CTE with an SCN range. This approximates
 * the two-phase LogMiner process: (1) starting a session with a set of logs,
 * and (2) querying {@code v$logmnr_contents} with a read SCN range.
 *
 * <p>The SQLite schema is designed to produce a {@link ResultSet} that is
 * compatible with {@link io.debezium.connector.oracle.logminer.events.LogMinerEventRow#fromResultSet}.
 *
 * <p>Usage:
 * <pre>
 *   try (LogMinerEventSimulator simulator = new LogMinerEventSimulator()) {
 *       simulator.loadCsv(getClass().getResourceAsStream("/concurrent/happy-path-template.csv"));
 *       List&lt;LogFile&gt; logs = simulator.getLogFiles();
 *       try (ResultSet rs = simulator.query(logs, Scn.valueOf(0), Scn.valueOf(10))) {
 *           while (rs.next()) {
 *               // process row
 *           }
 *       }
 *   }
 * </pre>
 *
 * @author Debezium Authors
 */
public class LogMinerEventSimulator implements AutoCloseable {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final Connection connection;
    private final Map<String, LogFile> logFilesByName = new HashMap<>();
    private final Set<String> createdTables = new HashSet<>();

    public LogMinerEventSimulator() throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:sqlite::memory:");
    }

    /**
     * Loads simulated LogMiner events from a CSV input stream.
     *
     * <p>The CSV format matches the template in
     * {@code src/test/resources/concurrent/happy-path-template.csv}.
     * Lines starting with {@code #} are treated as comments and ignored.
     *
     * @param inputStream the CSV data
     * @throws IOException if reading the stream fails
     * @throws SQLException if inserting into SQLite fails
     */
    public void loadCsv(InputStream inputStream) throws IOException, SQLException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String header = null;
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                if (header == null) {
                    header = line;
                    continue;
                }
                insertRow(header, line);
            }
        }
    }

    /**
     * Returns the set of {@link LogFile}s discovered from the loaded CSV.
     * The logs are sorted ascending by {@code firstScn}.
     */
    public List<LogFile> getLogFiles() {
        return logFilesByName.values().stream()
                .sorted(Comparator.comparing(LogFile::getFirstScn))
                .toList();
    }

    /**
     * Executes a simulated LogMiner query over the given logs and SCN range.
     *
     * <p>The query uses a CTE to union all selected log tables, then filters by
     * {@code SCN > readStartScn AND SCN <= readEndScn} and by operation codes
     * that match the buffered LogMiner query builder.
     *
     * @param logs the logs to include in the simulated session
     * @param readStartScn exclusive lower bound
     * @param readEndScn inclusive upper bound
     * @return a forward-only, read-only result set
     * @throws SQLException if the query fails
     */
    public ResultSet query(List<LogFile> logs, Scn readStartScn, Scn readEndScn) throws SQLException {
        if (logs.isEmpty()) {
            throw new IllegalArgumentException("At least one log must be specified");
        }

        StringBuilder sql = new StringBuilder(1024);
        sql.append("WITH logmnr_contents AS (");
        for (int i = 0; i < logs.size(); i++) {
            if (i > 0) {
                sql.append(" UNION ALL ");
            }
            String tableName = getTableNameForLog(logs.get(i).getFileName());
            sql.append("SELECT * FROM ").append(tableName);
        }
        sql.append(") ");
        sql.append("SELECT * FROM logmnr_contents ");
        sql.append("WHERE SCN > ? AND SCN <= ? ");
        sql.append("AND OPERATION_CODE IN (1, 2, 3, 5, 6, 7, 27, 34, 36, 255) ");
        sql.append("ORDER BY SCN, RS_ID");

        PreparedStatement stmt = connection.prepareStatement(
                sql.toString(),
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stmt.setLong(1, readStartScn.longValue());
        stmt.setLong(2, readEndScn.isNull() ? Long.MAX_VALUE : readEndScn.longValue());
        return stmt.executeQuery();
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    // ------------------------------------------------------------------ internal helpers

    private void insertRow(String header, String line) throws SQLException {
        String[] columns = header.split(",");
        String[] values = parseCsvLine(line);

        Map<String, String> row = new HashMap<>();
        for (int i = 0; i < columns.length && i < values.length; i++) {
            row.put(columns[i].trim(), values[i]);
        }

        String logName = row.get("log_name");
        String logType = row.get("log_type");
        Scn logFirstScn = parseScn(row.get("log_first_scn"));
        Scn logNextScn = parseScn(row.get("log_next_scn"));

        if (!createdTables.contains(logName)) {
            createLogTable(logName);
            createdTables.add(logName);

            LogFile logFile;
            if ("REDO".equalsIgnoreCase(logType)) {
                logFile = LogFile.forRedo(logName, logFirstScn, logNextScn,
                        java.math.BigInteger.ONE, false, parseInt(row.getOrDefault("thread", "1")), 1024L);
            }
            else {
                logFile = LogFile.forArchive(logName, logFirstScn, logNextScn,
                        java.math.BigInteger.ONE, parseInt(row.getOrDefault("thread", "1")), 1024L, false, false);
            }
            logFilesByName.put(logName, logFile);
        }

        String tableName = getTableNameForLog(logName);
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
        sql.append("SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, ");
        sql.append("USERNAME, ROW_ID, ROLLBACK, RS_ID, STATUS, INFO, SSN, THREAD, DATA_OBJ, DATA_OBJV, DATA_OBJD, ");
        sql.append("CLIENT_ID, START_SCN, COMMIT_SCN, START_TIMESTAMP, COMMIT_TIMESTAMP, SEQUENCE");
        sql.append(") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        try (PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
            stmt.setLong(1, parseLong(row.get("scn")));
            stmt.setString(2, row.get("sql_redo"));
            stmt.setInt(3, parseInt(row.get("operation_code")));
            stmt.setString(4, row.get("timestamp"));
            stmt.setString(5, row.get("transaction_id"));
            stmt.setInt(6, parseInt(row.getOrDefault("csf", "0")));
            stmt.setString(7, row.get("table_name"));
            stmt.setString(8, row.get("seg_owner"));
            stmt.setString(9, row.get("operation"));
            stmt.setString(10, row.get("username"));
            stmt.setString(11, row.get("row_id"));
            stmt.setInt(12, parseInt(row.getOrDefault("rollback_flag", "0")));
            stmt.setString(13, row.get("rs_id"));
            stmt.setInt(14, parseInt(row.getOrDefault("status", "0")));
            stmt.setString(15, row.get("info"));
            stmt.setLong(16, parseLong(row.getOrDefault("ssn", "0")));
            stmt.setInt(17, parseInt(row.getOrDefault("thread", "1")));
            stmt.setLong(18, parseLong(row.getOrDefault("data_obj", "0")));
            stmt.setLong(19, parseLong(row.getOrDefault("data_objv", "0")));
            stmt.setLong(20, parseLong(row.getOrDefault("data_objd", "0")));
            stmt.setString(21, row.get("client_id"));
            setScn(stmt, 22, row.get("start_scn"));
            setScn(stmt, 23, row.get("commit_scn"));
            stmt.setString(24, row.get("start_timestamp"));
            stmt.setString(25, row.get("commit_timestamp"));
            stmt.setLong(26, parseLong(row.getOrDefault("sequence", "0")));
            stmt.executeUpdate();
        }
    }

    private void createLogTable(String logName) throws SQLException {
        String tableName = getTableNameForLog(logName);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " (" +
                    "SCN INTEGER, " +
                    "SQL_REDO TEXT, " +
                    "OPERATION_CODE INTEGER, " +
                    "TIMESTAMP TEXT, " +
                    "XID TEXT, " +
                    "CSF INTEGER, " +
                    "TABLE_NAME TEXT, " +
                    "SEG_OWNER TEXT, " +
                    "OPERATION TEXT, " +
                    "USERNAME TEXT, " +
                    "ROW_ID TEXT, " +
                    "ROLLBACK INTEGER, " +
                    "RS_ID TEXT, " +
                    "STATUS INTEGER, " +
                    "INFO TEXT, " +
                    "SSN INTEGER, " +
                    "THREAD INTEGER, " +
                    "DATA_OBJ INTEGER, " +
                    "DATA_OBJV INTEGER, " +
                    "DATA_OBJD INTEGER, " +
                    "CLIENT_ID TEXT, " +
                    "START_SCN INTEGER, " +
                    "COMMIT_SCN INTEGER, " +
                    "START_TIMESTAMP TEXT, " +
                    "COMMIT_TIMESTAMP TEXT, " +
                    "SEQUENCE INTEGER" +
                    ")");
        }
    }

    private static String getTableNameForLog(String logName) {
        return "log_" + logName.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    private static void setScn(PreparedStatement stmt, int index, String value) throws SQLException {
        if (value == null || value.isEmpty()) {
            stmt.setNull(index, java.sql.Types.INTEGER);
        }
        else {
            stmt.setLong(index, Long.parseLong(value));
        }
    }

    private static long parseLong(String value) {
        if (value == null || value.isEmpty()) {
            return 0;
        }
        return Long.parseLong(value.trim());
    }

    private static int parseInt(String value) {
        if (value == null || value.isEmpty()) {
            return 0;
        }
        return Integer.parseInt(value.trim());
    }

    private static Scn parseScn(String value) {
        if (value == null || value.isEmpty()) {
            return Scn.NULL;
        }
        return Scn.valueOf(value.trim());
    }

    /**
     * Parses a simple CSV line. Does not handle embedded commas inside quotes
     * beyond the most basic case used by the template.
     */
    private static String[] parseCsvLine(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                }
                else {
                    inQuotes = !inQuotes;
                }
            }
            else if (c == ',' && !inQuotes) {
                result.add(current.toString().trim());
                current.setLength(0);
            }
            else {
                current.append(c);
            }
        }
        result.add(current.toString().trim());
        return result.toArray(new String[0]);
    }
}
