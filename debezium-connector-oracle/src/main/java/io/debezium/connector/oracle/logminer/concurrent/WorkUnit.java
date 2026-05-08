/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.concurrent;

import java.util.List;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogFile;

/**
 * An immutable descriptor for a single unit of work submitted to a {@link LogMinerWorker}.
 *
 * <p>The distinction between the two types of work unit is:
 * <ul>
 *   <li>{@link WorkUnitType#WORKER}: clean archive-log slice; {@code sessionStartScn == readStartScn},
 *       no {@code inheritedTransactions}.</li>
 *   <li>{@link WorkUnitType#BRIDGE}: cross-log resolution with a known tight upper cap;
 *       {@code sessionStartScn < readStartScn} (session reaches back); {@code readEndScn} is
 *       the exact commit SCN of the oldest/latest orphan being resolved.</li>
 * </ul>
 *
 * @author Debezium Authors
 */
public record WorkUnit(
        WorkUnitType type,

        /**
         * All log files that must be registered with the LogMiner session.
         * For BRIDGE, this includes prior logs needed to satisfy the Golden Rule
         * (session must cover the transaction START), plus the logs being read.
         */
        List<LogFile> logFiles,

        /**
         * The SCN passed to {@code DBMS_LOGMNR.START_LOGMNR} as {@code startScn}.
         * Must be {@code <=} the {@code startScn} of every transaction in
         * {@code inheritedTransactions} to satisfy the Golden Rule.
         */
        Scn sessionStartScn,

        /**
         * The SCN passed to {@code DBMS_LOGMNR.START_LOGMNR} as {@code endScn}.
         * For BRIDGE: the exact commit SCN of the matched orphan (tight cap).
         * For WORKER: the last log's {@code nextScn}.
         */
        Scn sessionEndScn,

        /**
         * Exclusive lower bound for the {@code v$logmnr_contents} WHERE clause.
         * The worker reads {@code scn > readStartScn}. For BRIDGE this equals
         * {@code inheritedTransactions.get(i).lastReadScn()} (i.e. the furthest point
         * already read for these transactions, so no data is re-read).
         */
        Scn readStartScn,

        /**
         * Inclusive upper bound for the {@code v$logmnr_contents} WHERE clause.
         * The worker reads {@code scn <= readEndScn}.
         */
        Scn readEndScn,

        /**
         * Transactions that were started in prior logs whose DML events have already been
         * partially accumulated. The worker pre-populates its transaction cache from these
         * before executing the query, so that when the COMMIT arrives it can be merged
         * with the already-accumulated events.
         * Empty for {@link WorkUnitType#WORKER} units.
         */
        List<InheritedTransaction> inheritedTransactions) {

    /**
    * Convenience factory for a clean WORKER unit that includes the first SCN in the assigned
    * logs by using an exclusive lower bound one SCN earlier.
     *
     * @param logFiles the log files assigned to this worker
     * @param firstScn the {@code firstScn} of the first assigned log
     * @param nextScn  the {@code nextScn} of the last assigned log (exclusive upper bound)
     * @param readStartScn the exclusive lower bound for the worker query
     * @return a new WORKER {@link WorkUnit}
     */
    public static WorkUnit worker(List<LogFile> logFiles, Scn firstScn, Scn nextScn, Scn readStartScn) {
        return new WorkUnit(WorkUnitType.WORKER, logFiles, firstScn, nextScn, readStartScn, nextScn, List.of());
    }
}
