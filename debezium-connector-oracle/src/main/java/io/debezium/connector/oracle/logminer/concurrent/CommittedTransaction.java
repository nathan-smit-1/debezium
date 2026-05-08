/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.concurrent;

import java.time.Instant;
import java.util.List;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;

/**
 * A fully resolved transaction that is ready for dispatch to the downstream event pipeline.
 *
 * <p>Produced by {@link LogMinerWorker} instances and collected by the
 * {@link LogMinerWorkerCoordinator} for SCN-ordered dispatch.
 *
 * <p>The {@code commitRsId} field is the {@code RS_ID} value from the COMMIT row in
 * {@code v$logmnr_contents}. It is used as a tie-breaker when two transactions share the same
 * {@code commitScn}, preserving the same relative order that single-threaded LogMiner processing
 * would produce (which is ordered by {@code scn, rs_id}).
 *
 * @author Debezium Authors
 */
public record CommittedTransaction(
        String transactionId,
        Scn startScn,
        Scn commitScn,
        Instant commitTime,
        String userName,
        String clientId,
        int redoThreadId,

        /**
         * RS_ID from the COMMIT row in v$logmnr_contents. Used as a secondary sort key when
         * two transactions share the same commitScn. May be null for synthesized transactions.
         */
        String commitRsId,

        /** All DML events for this transaction, in ascending SCN order. */
        List<LogMinerEvent> events) {
}
