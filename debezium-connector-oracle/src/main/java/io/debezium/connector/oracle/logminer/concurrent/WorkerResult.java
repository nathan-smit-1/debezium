/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.concurrent;

import java.util.List;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogMinerSchemaChangeRecord;

/**
 * The immutable result returned by a {@link LogMinerWorker} after completing its assigned
 * {@link WorkUnit}.
 *
 * <p>The coordinator uses this result to:
 * <ul>
 *   <li>Collect {@link #resolvedTransactions()} for SCN-ordered dispatch.</li>
 *   <li>Forward {@link #unresolvedTransactions()} as {@link InheritedTransaction} records to
 *       subsequent BRIDGE or serial-continuation workers.</li>
 *   <li>Match {@link #orphanCommits()} against pending inherited transactions to plan
 *       BRIDGE units.</li>
 *   <li>Apply {@link #schemaChanges()} in SCN order before dispatching DML events.</li>
 * </ul>
 *
 * @author Debezium Authors
 */
public record WorkerResult(
        /** Zero-based index of the worker that produced this result. */
        int workerId,

        WorkUnitType unitType,

        /**
         * Transactions whose COMMIT was found within this worker's read range.
         * Sorted ascending by {@code commitScn}.
         */
        List<CommittedTransaction> resolvedTransactions,

        /**
         * Transactions whose START was found (either injected or discovered) but whose COMMIT
         * was not found within this worker's read range.
         * These become {@link InheritedTransaction} records for the next wave.
         */
        List<InheritedTransaction> unresolvedTransactions,

        /**
         * COMMIT events encountered for transactions whose START was not found in this worker's
         * read range and was not present in the injected inherited-transaction list.
         */
        List<OrphanCommit> orphanCommits,

        /**
         * DDL / schema-change events encountered in SCN order. These must be applied to the
         * database schema before any DML event at a higher SCN is dispatched.
         */
        List<LogMinerSchemaChangeRecord> schemaChanges,

        /**
         * The exclusive lower bound actually used for this worker's query.
         *
         * <p>The worker reads {@code scn > readStartScn}. Keeping this exact boundary allows the
         * coordinator to detect whether a later known-commit continuation overlaps this worker's
         * read slice, instead of approximating overlap from the assigned log set.
         */
        Scn readStartScn,

        /**
         * The {@code firstScn} of the first log assigned to this worker's unit.
         * Used by the coordinator to correlate orphan commits with prior workers.
         */
        Scn logFirstScn,

        /**
         * The last SCN actually read ({@code = readEndScn} of the {@link WorkUnit} unless the
         * worker terminated early). Used to set {@code lastReadScn} on forwarded
         * {@link InheritedTransaction} records.
         */
        Scn finalReadScn) {

    /** Convenience factory for a result that resolved every inherited transaction it was given. */
    public static WorkerResult fullyResolved(int workerId, WorkUnitType type,
                                             List<CommittedTransaction> resolved, Scn logFirstScn, Scn finalReadScn) {
        return new WorkerResult(workerId, type, resolved, List.of(), List.of(), List.of(),
                logFirstScn.subtract(Scn.ONE), logFirstScn, finalReadScn);
    }

}
