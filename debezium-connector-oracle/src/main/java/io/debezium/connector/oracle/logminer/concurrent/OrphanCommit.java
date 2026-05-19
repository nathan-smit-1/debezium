/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.concurrent;

import java.time.Instant;

import io.debezium.connector.oracle.Scn;

/**
 * A COMMIT event encountered by a worker for which no matching START was observed within that
 * worker's read range and which was not present in the worker's inherited transaction list.
 *
 * <p>Orphan commits arise when a transaction started in log N is committed in log M (M > N) and
 * the worker assigned to log M had no knowledge of the transaction's START. The
 * {@link LogMinerWorkerCoordinator} matches orphan commits against
 * {@link InheritedTransaction} records to plan {@link WorkUnitType#BRIDGE} units.
 *
 * @author Debezium Authors
 */
public record OrphanCommit(
        String transactionId,
        Scn commitScn,
        Instant commitTime,
        int redoThreadId,

        /** The {@code firstScn} of the log in which this commit was found. */
        Scn logFirstScn) {
}
