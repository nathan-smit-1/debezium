/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.concurrent;

import java.util.List;

import io.debezium.connector.oracle.logminer.LogFile;

/**
 * Package-private utility providing shared log/transaction description helpers for the
 * concurrent LogMiner worker classes.
 */
final class LogMinerWorkerDescriptions {

    private LogMinerWorkerDescriptions() {
    }

    static String describeLogs(List<LogFile> logs) {
        if (logs == null || logs.isEmpty()) {
            return "[]";
        }
        return logs.stream()
                .map(log -> log.getFileName() + "[" + log.getFirstScn() + "," + log.getNextScn() + ")")
                .toList()
                .toString();
    }

    static String describeTransactionIds(List<String> transactionIds) {
        if (transactionIds == null || transactionIds.isEmpty()) {
            return "[]";
        }
        return transactionIds.toString();
    }
}
