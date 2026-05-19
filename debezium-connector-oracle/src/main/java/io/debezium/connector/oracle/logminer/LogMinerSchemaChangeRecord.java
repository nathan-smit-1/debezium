/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;
import io.debezium.relational.TableId;

/**
 * Container for a DDL event captured during mining.
 *
 * @author Debezium Authors
 */
public record LogMinerSchemaChangeRecord(
        Scn scn,
        TableId tableId,
        String redoSql,
        Instant changeTime,
        int redoThread,
        String rsId,
        long transactionSequence,
        Long objectId) {
}
