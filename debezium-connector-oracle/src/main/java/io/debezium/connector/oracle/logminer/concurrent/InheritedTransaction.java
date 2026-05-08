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
 * A transaction that started in a prior worker's log range and has not yet been committed.
 *
 * <p>Carried across waves by the {@link LogMinerWorkerCoordinator} and injected as pre-seeded
 * cache entries into subsequent {@link LogMinerWorker} instances that continue the transaction.
 *
 * <p>{@code trustedPrefixScn} is the highest SCN below which the transaction prefix is known to be
 * complete and safe to skip when a known-commit continuation is planned. This is fixed when the
 * transaction first becomes inherited and must not move forward as the transaction is handed across
 * additional unresolved workers.
 *
 * <p>{@code lastReadScn} is the furthest SCN read so far for this transaction. It remains the key
 * field for unknown-commit continuation and safe-horizon computation.
 *
 * @author Debezium Authors
 */
public record InheritedTransaction(
        String transactionId,

        /** The SCN of the START event observed in the original log. */
        Scn startScn,

        /** The change time of the START event. */
        Instant startTime,

        String userName,
        String clientId,
        int redoThreadId,

        /**
         * The highest SCN below which the transaction prefix is trusted complete.
         * Known-commit continuation may read strictly above this boundary.
         */
        Scn trustedPrefixScn,

        /**
         * The furthest SCN that has already been queried for this transaction's DML.
         * A continuation worker uses this as its exclusive {@code readStartScn} lower bound.
         */
        Scn lastReadScn,

        /**
         * All DML events accumulated so far for this transaction, in SCN ascending order.
         * The continuation worker appends any additional DML events found in its read range.
         */
        List<LogMinerEvent> events) {
}
