/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.SQLException;

import org.slf4j.Logger;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;

public final class LogMinerSessionHelper {

    private LogMinerSessionHelper() {
    }

    public static void configureSession(OracleConnection connection, OracleConnectorConfig connectorConfig, Logger logger) throws SQLException {
        connection.executeWithoutCommitting("ALTER SESSION SET"
                + "  NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
                + "  NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'"
                + "  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM'"
                + "  NLS_NUMERIC_CHARACTERS = '.,'");
        connection.executeWithoutCommitting("ALTER SESSION SET TIME_ZONE = '00:00'");

        final long hashAreaSize = connectorConfig.getLogMiningHashAreaSize();
        if (hashAreaSize > 0) {
            logger.debug("Setting LogMiner connection HASH_AREA_SIZE={}", hashAreaSize);
            connection.executeWithoutCommitting("ALTER SESSION SET HASH_AREA_SIZE = " + hashAreaSize);
        }

        final long sortAreaSize = connectorConfig.getLogMiningSortAreaSize();
        if (sortAreaSize > 0) {
            logger.debug("Setting LogMiner connection SORT_AREA_SIZE={}", sortAreaSize);
            connection.executeWithoutCommitting("ALTER SESSION SET SORT_AREA_SIZE = " + sortAreaSize);
        }
    }
}