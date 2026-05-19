/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.concurrent;

import static io.debezium.connector.oracle.logminer.concurrent.LogMinerWorkerDescriptions.describeLogs;
import static io.debezium.connector.oracle.logminer.concurrent.LogMinerWorkerDescriptions.describeTransactionIds;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.AbstractOracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.TruncateReceiver;
import io.debezium.connector.oracle.logminer.LogFile;
import io.debezium.connector.oracle.logminer.LogMinerEventDispatchHelper;
import io.debezium.connector.oracle.logminer.TransactionCommitConsumer;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;

/**
 * Coordinates concurrent {@link LogMinerWorker} instances across a wave of archive log files.
 *
 * <h3>Architecture</h3>
 * <p>
 * A "wave" is a set of archive log files available since the last coordination cycle. The
 * coordinator partitions those logs into {@link WorkUnit}s:
 * <ul>
 *   <li><b>WORKER</b> units ╬ô├ç├╢ one per log (or one per group of logs if N &lt; log count).
 *       Workers run in parallel on independent log slices.</li>
 *   <li><b>BRIDGE</b> units ╬ô├ç├╢ created after workers complete, for orphan commits that have a
 *       known matching {@link InheritedTransaction}. Uses a tight read cap equal to the commit
 *       SCN. Runs in the next wave if the match was found in this wave's results.</li>
 * </ul>
 *
 * <h3>Cross-wave state</h3>
 * <ul>
 *   <li>{@link #pendingInheritedTransactions} ╬ô├ç├╢ open transactions carried from prior waves</li>
 *   <li>{@link #pendingOrphanCommits} ╬ô├ç├╢ orphan commits waiting to be matched with an inherited tx</li>
 * </ul>
 *
 * <h3>Dispatch ordering</h3>
 * <p>Resolved transactions from all workers in a wave are merge-sorted by {@code commitScn} before
 * being dispatched to the downstream pipeline. Schema changes are applied before any DML event at
 * a higher SCN.
 *
 * <h3>Activation gates</h3>
 * <ol>
 *   <li>{@code log.mining.concurrent.readers > 1} ╬ô├ç├╢ must be configured; checked in
 *       {@link io.debezium.connector.oracle.logminer.buffered.BufferedLogMinerAdapter}.</li>
 *   <li>Only archive logs in scope ╬ô├ç├╢ if any online redo log is in the current wave's log set, fall
 *       back to single-threaded mode for that wave.</li>
 *   <li>At least 2 archive logs in scope ╬ô├ç├╢ if only 1, fall back to single-threaded mode.</li>
 * </ol>
 *
 * @author Debezium Authors
 */
public class LogMinerWorkerCoordinator implements AutoCloseable {

    @FunctionalInterface
    protected interface WorkerResultConsumer {
        void accept(WorkerResult result) throws InterruptedException;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerWorkerCoordinator.class);
    private static final int EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 30;

    private final OracleConnectorConfig connectorConfig;
    private final OracleDatabaseSchema schema;
    private final JdbcConfiguration jdbcConfig;
    private final int concurrentReaders;
    private final ExecutorService executorService;
    private final AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics;

    /** Open transactions whose STARTs are in prior logs and whose COMMITs have not yet been found. */
    private final List<InheritedTransaction> pendingInheritedTransactions = new ArrayList<>();

    /** COMMIT events whose STARTs were not found in the log that contained the COMMIT. */
    private final List<OrphanCommit> pendingOrphanCommits = new ArrayList<>();

    /** Replay units for overlapping worker tails that must be re-mined after bridge resolution. */
    private final List<WorkUnit> pendingReplayUnits = new ArrayList<>();

    /**
     * Resolved transactions that were held back by the safe-dispatch horizon in a prior wave
     * because a cross-log transaction could have committed at a lower SCN. These are prepended
     * to the next wave's dispatch list so that global SCN order is preserved.
     */
    private final List<CommittedTransaction> pendingDispatch = new ArrayList<>();

    /**
     * Schema change records held back above the safe dispatch horizon in a prior wave.
     * These are prepended to the next wave's schema change list so that DDLs are applied in
     * the correct SCN order before any DML events that follow them.
     */
    private final List<WorkerResult.SchemaChangeRecord> pendingSchemaChanges = new ArrayList<>();

    /**
     * Work units scheduled for a targeted DDL-contamination replay. Workers re-mine a narrow
     * log range after the wave's schema changes have been applied, so that DML events that were
     * parsed against a stale schema are re-processed with the correct column layout.
     */
    private final List<WorkUnit> pendingDdlContaminationUnits = new ArrayList<>();

    /**
     * Transaction IDs of committed transactions whose DML events were parsed against a stale
     * schema (before a DDL in the same wave updated the table). Only these transactions are
     * dispatched from the DDL-contamination replay, preventing double-dispatch of all other
     * transactions that happen to fall inside the replay log range.
     */
    private final Set<String> ddlContaminatedTransactionIds = new HashSet<>();

    private final AtomicInteger nextWorkerIndex = new AtomicInteger();

    private final WorkUnitPlanner planner;
    private final TransactionMerger merger;

    public LogMinerWorkerCoordinator(OracleConnectorConfig connectorConfig,
                                     OracleDatabaseSchema schema,
                                     JdbcConfiguration jdbcConfig,
                                     AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics) {
        this.connectorConfig = connectorConfig;
        this.schema = schema;
        this.jdbcConfig = jdbcConfig;
        this.streamingMetrics = streamingMetrics;
        this.concurrentReaders = connectorConfig.getLogMiningConcurrentReaders();
        this.executorService = Executors.newFixedThreadPool(concurrentReaders, r -> {
            final int workerId = nextWorkerIndex.getAndIncrement();
            final Thread t = new Thread(r, "debezium-logminer-worker-" + workerId);
            t.setDaemon(true);
            return t;
        });
        this.planner = new WorkUnitPlanner(concurrentReaders);
        this.merger = new TransactionMerger(connectorConfig, schema, streamingMetrics,
                pendingInheritedTransactions, pendingOrphanCommits, pendingReplayUnits,
                pendingDispatch, pendingSchemaChanges, pendingDdlContaminationUnits,
                ddlContaminatedTransactionIds, planner);
    }

    // ------------------------------------------------------------------ public API

    /**
     * Returns {@code true} if concurrent reading is appropriate for the given log set.
     * Falls back to single-threaded mode when online redo logs are in scope or fewer than
     * 2 archive logs are available.
     *
     * @param logs the log files selected for this wave
     * @return {@code true} if concurrent reading should be used
     */
    public boolean isConcurrentReadingApplicable(List<LogFile> logs) {
        final boolean anyRedoLog = logs.stream().anyMatch(LogFile::isRedo);
        if (anyRedoLog) {
            LOGGER.debug("Concurrent reading skipped: online redo log(s) in scope.");
            return false;
        }
        final long archiveCount = logs.stream().filter(LogFile::isArchive).count();
        if (archiveCount < 2) {
            LOGGER.debug("Concurrent reading skipped: only {} archive log(s) in scope.", archiveCount);
            return false;
        }
        return true;
    }

    /**
     * Runs a full coordination wave over the given archive log files.
     *
     * <p>This method:
     * <ol>
    *   <li>Partitions the log list into {@link WorkUnitType#WORKER} units plus any pending
    *       BRIDGE units.</li>
     *   <li>Submits all units to the thread pool and waits for all to complete.</li>
     *   <li>Merges all results in SCN order.</li>
     *   <li>Applies schema changes, then dispatches committed transactions.</li>
     *   <li>Updates {@link #pendingInheritedTransactions} and {@link #pendingOrphanCommits}
     *       for the next wave.</li>
     * </ol>
     *
     * @param archiveLogs the archive logs to mine in this wave
     * @param dispatcher  the downstream event dispatcher
     * @param partition   the Oracle partition context
     * @param offsetContext the current offset context (updated in-place by dispatch)
     * @param databaseOffset the database server's timezone offset for time adjustment
     * @return the highest SCN that was fully processed in this wave
     * @throws InterruptedException if the thread is interrupted while waiting for workers
     */
    public Scn executeWave(
                           Scn readStartScn,
                           List<LogFile> archiveLogs,
                           EventDispatcher<OraclePartition, TableId> dispatcher,
                           OraclePartition partition,
                           OracleOffsetContext offsetContext,
                           ZoneOffset databaseOffset)
            throws InterruptedException {

        // Sort archive logs ascending by firstScn
        final List<LogFile> sortedLogs = archiveLogs.stream()
                .sorted(Comparator.comparing(LogFile::getFirstScn))
                .toList();

        // Snapshot pending state before building work units, since buildBridgeUnits mutates
        // pendingInheritedTransactions/pendingOrphanCommits before workers execute.
        // If workers fail, this snapshot is used to roll back state.
        final List<InheritedTransaction> savedInherited = new ArrayList<>(pendingInheritedTransactions);
        final List<OrphanCommit> savedOrphans = new ArrayList<>(pendingOrphanCommits);
        final List<CommittedTransaction> savedPendingDispatch = new ArrayList<>(pendingDispatch);
        final List<WorkerResult.SchemaChangeRecord> savedPendingSchemaChanges = new ArrayList<>(pendingSchemaChanges);
        final List<WorkUnit> savedPendingReplayUnits = new ArrayList<>(pendingReplayUnits);

        // Build work units
        final List<WorkUnit> units = planner.buildWorkUnits(sortedLogs, readStartScn, pendingInheritedTransactions, pendingOrphanCommits);

        LOGGER.info("Starting concurrent wave: logs={}, workUnits={}, workersConfigured={}, pendingInherited={}, pendingOrphans={}, pendingDispatch={}",
                describeLogs(sortedLogs), units.size(), concurrentReaders,
                savedInherited.size(), savedOrphans.size(), pendingDispatch.size());
        planner.logPlannedWorkUnits(units);

        final List<WorkerResult> results = new ArrayList<>(units.size());
        final List<WorkerResult.SchemaChangeRecord> appliedWaveSchemaChanges = new ArrayList<>();
        try {
            executeWorkersAsCompleted(units, results::add);

            // Merge only after the full wave completes so the safe-dispatch horizon is computed
            // from the complete orphan/unresolved picture for the wave, not a partial prefix.
            results.sort(Comparator.comparingInt(WorkerResult::workerId));

            final Scn processedScn = mergeCompletedWorkerPrefix(
                    sortedLogs,
                    results,
                    dispatcher,
                    partition,
                    offsetContext,
                    databaseOffset,
                    false,
                    appliedWaveSchemaChanges);

            final List<WorkUnit> knownCommitReplayUnits = planner.planReplayWorkUnits(sortedLogs, results,
                    planner.planKnownCommitContinuations(pendingInheritedTransactions, pendingOrphanCommits));
            for (WorkUnit replayUnit : knownCommitReplayUnits) {
                if (!pendingReplayUnits.contains(replayUnit)) {
                    pendingReplayUnits.add(replayUnit);
                }
            }

            LOGGER.info("Concurrent wave worker execution complete: results={}", describeResults(results));
            return processedScn;
        }
        catch (Exception e) {
            // buildBridgeUnits may have already removed items from pending state;
            // restore the snapshot so the next wave can retry with the original state.
            pendingInheritedTransactions.clear();
            pendingInheritedTransactions.addAll(savedInherited);
            pendingOrphanCommits.clear();
            pendingOrphanCommits.addAll(savedOrphans);
            pendingDispatch.clear();
            pendingDispatch.addAll(savedPendingDispatch);
            pendingSchemaChanges.clear();
            pendingSchemaChanges.addAll(savedPendingSchemaChanges);
            pendingReplayUnits.clear();
            pendingReplayUnits.addAll(savedPendingReplayUnits);
            throw e;
        }
    }

    public Scn executeWave(
                           List<LogFile> archiveLogs,
                           EventDispatcher<OraclePartition, TableId> dispatcher,
                           OraclePartition partition,
                           OracleOffsetContext offsetContext,
                           ZoneOffset databaseOffset)
            throws InterruptedException {
        final Scn defaultReadStartScn = archiveLogs.stream()
                .map(LogFile::getFirstScn)
                .min(Comparator.naturalOrder())
                .orElse(Scn.NULL);
        return executeWave(defaultReadStartScn, archiveLogs, dispatcher, partition, offsetContext, databaseOffset);
    }

    /**
     * Runs a <strong>serial</strong> coordination wave ╬ô├ç├╢ exactly one worker processes all logs.
     * Used as a fallback when concurrent reading is not applicable (online redo logs in scope
     * or fewer than 2 archive logs available).
     *
     * <p>Unlike {@link #executeWave}, this method runs the single worker directly on the
     * calling thread ╬ô├ç├╢ it does not use the thread pool. This guarantees that online redo
     * logs are never mined concurrently with other workers.
     *
     * <p>The single worker is given all pending inherited transactions so that cross-wave
     * resolution still occurs correctly in serial mode.
     *
     * @param allLogs all logs (archive + redo) in scope for this wave
     * @param dispatcher the downstream event dispatcher
     * @param partition the Oracle partition context
     * @param offsetContext the current offset context
     * @param databaseOffset the database server's timezone offset for time adjustment
     * @return the highest SCN that was fully processed
     * @throws RuntimeException if the worker fails
     */
    public Scn executeSerial(
                             Scn readStartScn,
                             Scn readEndScn,
                             List<LogFile> allLogs,
                             EventDispatcher<OraclePartition, TableId> dispatcher,
                             OraclePartition partition,
                             OracleOffsetContext offsetContext,
                             ZoneOffset databaseOffset) {

        final WorkUnit unit = planner.buildSerialWorkUnit(allLogs, readStartScn, readEndScn, pendingInheritedTransactions);

        LOGGER.info("Starting serial wave: logs={}, session=[{},{}], read=[{},{}], sessionSpan={}, readSpan={}, sessionReadGap={}, inheritedTransactions={}",
                describeLogs(unit.logFiles()), unit.sessionStartScn(), unit.sessionEndScn(),
                unit.readStartScn(), unit.readEndScn(),
                describeScnSpan(unit.sessionStartScn(), unit.sessionEndScn()),
                describeScnSpan(unit.readStartScn(), unit.readEndScn()),
                describeScnGap(unit.sessionStartScn(), unit.readStartScn()),
                describeTransactionIds(pendingInheritedTransactions.stream()
                        .map(InheritedTransaction::transactionId)
                        .toList()));

        try {
            merger.dispatchPendingBeforeSerial(dispatcher, partition, offsetContext, databaseOffset);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Dispatch interrupted before serial wave", e);
        }

        final Set<String> resolvedTransactionIds = new HashSet<>();

        final WorkerResult result;
        try {
            result = new LogMinerWorker(
                    0,
                    unit,
                    connectorConfig,
                    schema,
                    jdbcConfig,
                    transaction -> {
                        resolvedTransactionIds.add(transaction.transactionId());
                        merger.dispatchCommittedTransaction(transaction, dispatcher, partition, offsetContext, databaseOffset);
                    },
                    change -> merger.applySchemaChange(change, dispatcher, partition, offsetContext))
                    .call();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Serial LogMiner worker interrupted", e);
        }
        catch (Exception e) {
            if (e instanceof RuntimeException rte) {
                throw rte;
            }
            throw new RuntimeException("Serial LogMiner worker failed", e);
        }

        pendingInheritedTransactions.removeIf(tx -> resolvedTransactionIds.contains(tx.transactionId()));
        pendingOrphanCommits.removeIf(orphan -> resolvedTransactionIds.contains(orphan.transactionId()));
        merger.updateCrossWaveState(result);

        if (!result.finalReadScn().isNull()) {
            try {
                offsetContext.setScn(result.finalReadScn());
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Dispatch interrupted during serial wave", e);
            }
        }

        LOGGER.info("Serial wave complete: dispatched={}, unresolvedNow={}, orphansNow={}, nextScn={}",
                resolvedTransactionIds.size(),
                pendingInheritedTransactions.size(),
                pendingOrphanCommits.size(),
                result.finalReadScn());

        return result.finalReadScn();
    }

    public Scn executeSerial(
                             List<LogFile> allLogs,
                             EventDispatcher<OraclePartition, TableId> dispatcher,
                             OraclePartition partition,
                             OracleOffsetContext offsetContext,
                             ZoneOffset databaseOffset) {
        final Scn defaultReadStartScn = allLogs.stream()
                .map(LogFile::getFirstScn)
                .min(Comparator.naturalOrder())
                .orElse(Scn.NULL);
        final Scn defaultReadEndScn = allLogs.stream()
                .map(LogFile::getNextScn)
                .max(Comparator.naturalOrder())
                .orElse(Scn.NULL);
        return executeSerial(defaultReadStartScn, defaultReadEndScn, allLogs, dispatcher, partition, offsetContext, databaseOffset);
    }


    private static String describeScnSpan(Scn startScn, Scn endScn) {
        if (startScn.isNull() || endScn.isNull()) {
            return "unknown";
        }
        return endScn.subtract(startScn).toString();
    }

    private static String describeScnGap(Scn lowerScn, Scn upperScn) {
        if (lowerScn.isNull() || upperScn.isNull()) {
            return "unknown";
        }
        if (upperScn.compareTo(lowerScn) <= 0) {
            return "0";
        }
        return upperScn.subtract(lowerScn).toString();
    }

    /**
    * Returns the list of pending inherited transactions (open transactions carried across waves).
    * Used by the streaming source to decide whether unknown-commit serial fallback is required.
     */
    public List<InheritedTransaction> getPendingInheritedTransactions() {
        return List.copyOf(pendingInheritedTransactions);
    }

    /**
     * Returns the list of pending orphan commits (commits with no known START yet).
     */
    public List<OrphanCommit> getPendingOrphanCommits() {
        return List.copyOf(pendingOrphanCommits);
    }

    /**
     * Returns whether there are unresolved inherited transactions whose commit location is still
     * unknown.
     *
     * <p>These are inherited transactions that do not yet have a matching orphan commit. When
     * present, the concurrent source should fall back to serial processing so a single worker owns
     * continuation until the commit is found.
     */
    public boolean hasPendingUnknownCommitTransactions() {
        if (pendingInheritedTransactions.isEmpty()) {
            return false;
        }

        final Set<String> orphanTransactionIds = pendingOrphanCommits.stream()
                .map(OrphanCommit::transactionId)
                .collect(Collectors.toSet());

        return pendingInheritedTransactions.stream()
                .anyMatch(transaction -> !orphanTransactionIds.contains(transaction.transactionId()));
    }

    public boolean hasPendingBridgeTransactions() {
        if (pendingInheritedTransactions.isEmpty() || pendingOrphanCommits.isEmpty()) {
            return false;
        }

        final Set<String> orphanTransactionIds = pendingOrphanCommits.stream()
                .map(OrphanCommit::transactionId)
                .collect(Collectors.toSet());

        return pendingInheritedTransactions.stream()
                .map(InheritedTransaction::transactionId)
                .anyMatch(orphanTransactionIds::contains);
    }

    public boolean hasPendingReplayUnits() {
        return !pendingReplayUnits.isEmpty();
    }

    /**
     * Returns {@code true} if there are DDL-contaminated transactions waiting for a targeted
     * replay pass to re-dispatch them after the wave's schema changes have been applied.
     */
    public boolean hasPendingDdlContaminationReplay() {
        return !pendingDdlContaminationUnits.isEmpty();
    }

    /**
     * Executes a targeted replay for transactions whose DML events were parsed against a stale
     * schema because a DDL for the same table occurred at a lower SCN in the same concurrent wave.
     *
     * <p>The schema has already been updated by the time this method is called (during the main
     * wave's {@code mergeAndDispatch}). Workers re-mine the relevant log range and the coordinator
     * dispatches <em>only</em> the quarantined transactions, discarding all others to prevent
     * double-dispatch of transactions that were already sent in the main wave.
     *
     * @param dispatcher    the downstream event dispatcher
     * @param partition     the Oracle partition context
     * @param offsetContext the current offset context (updated in-place)
     * @param databaseOffset the database server timezone offset
     * @return the highest commit SCN dispatched, or {@link Scn#NULL} if nothing was dispatched
     * @throws InterruptedException if the calling thread is interrupted while waiting for workers
     */
    public Scn executeDdlContaminationReplay(EventDispatcher<OraclePartition, TableId> dispatcher,
                                             OraclePartition partition,
                                             OracleOffsetContext offsetContext,
                                             ZoneOffset databaseOffset)
            throws InterruptedException {
        if (pendingDdlContaminationUnits.isEmpty()) {
            return Scn.NULL;
        }

        final List<WorkUnit> units = List.copyOf(pendingDdlContaminationUnits);
        pendingDdlContaminationUnits.clear();
        final Set<String> targetIds = Set.copyOf(ddlContaminatedTransactionIds);
        ddlContaminatedTransactionIds.clear();

        LOGGER.info("Executing DDL-contamination replay: {} work unit(s) targeting {} transaction(s): {}",
                units.size(), targetIds.size(),
                describeTransactionIds(List.copyOf(targetIds)));
        planner.logPlannedWorkUnits(units);

        final List<WorkerResult> results = executeWorkers(units);

        // Collect only the quarantined transactions; discard everything else to prevent
        // double-dispatch of transactions that were already dispatched in the main wave.
        final List<CommittedTransaction> replayed = results.stream()
                .flatMap(r -> r.resolvedTransactions().stream())
                .filter(tx -> targetIds.contains(tx.transactionId()))
                .sorted(Comparator.comparing(CommittedTransaction::commitScn)
                        .thenComparing(CommittedTransaction::commitRsId,
                                Comparator.nullsLast(Comparator.naturalOrder())))
                .toList();

        if (replayed.isEmpty()) {
            LOGGER.warn("DDL-contamination replay found no matching transactions for target IDs: {}", targetIds);
            return Scn.NULL;
        }

        LOGGER.info("DDL-contamination replay dispatching {} re-parsed transaction(s): {}",
                replayed.size(),
                describeTransactionIds(replayed.stream().map(CommittedTransaction::transactionId).toList()));

        Scn maxScn = Scn.NULL;
        for (final CommittedTransaction tx : replayed) {
            merger.dispatchCommittedTransaction(tx, dispatcher, partition, offsetContext, databaseOffset);
            if (maxScn.isNull() || tx.commitScn().compareTo(maxScn) > 0) {
                maxScn = tx.commitScn();
            }
        }
        return maxScn;
    }

    public Scn executePendingBridges(List<LogFile> archiveLogs,
                                     EventDispatcher<OraclePartition, TableId> dispatcher,
                                     OraclePartition partition,
                                     OracleOffsetContext offsetContext,
                                     ZoneOffset databaseOffset)
            throws InterruptedException {

        final List<LogFile> sortedLogs = archiveLogs.stream()
                .sorted(Comparator.comparing(LogFile::getFirstScn))
                .toList();

        final List<InheritedTransaction> savedInherited = new ArrayList<>(pendingInheritedTransactions);
        final List<OrphanCommit> savedOrphans = new ArrayList<>(pendingOrphanCommits);

        final List<WorkUnit> bridgeUnits = planner.buildBridgeUnits(sortedLogs, pendingInheritedTransactions, pendingOrphanCommits);
        if (bridgeUnits.isEmpty()) {
            return Scn.NULL;
        }

        LOGGER.info("Starting bridge follow-up wave: logs={}, workUnits={}, pendingInherited={}, pendingOrphans={}, pendingDispatch={}",
                describeLogs(sortedLogs), bridgeUnits.size(),
                savedInherited.size(), savedOrphans.size(), pendingDispatch.size());
        planner.logPlannedWorkUnits(bridgeUnits);

        final List<WorkerResult> results;
        try {
            results = executeWorkers(bridgeUnits);
        }
        catch (Exception e) {
            pendingInheritedTransactions.clear();
            pendingInheritedTransactions.addAll(savedInherited);
            pendingOrphanCommits.clear();
            pendingOrphanCommits.addAll(savedOrphans);
            throw e;
        }

        LOGGER.info("Bridge follow-up worker execution complete: results={}", describeResults(results));
        // The bridge rereads the unsafe interval that originally motivated any known-commit
        // replay tail. Transactions beyond that boundary are already preserved in pendingDispatch,
        // so replay units planned from the earlier worker wave are stale once the bridge succeeds.
        pendingReplayUnits.clear();
        return merger.mergeAndDispatch(sortedLogs, results, dispatcher, partition, offsetContext, databaseOffset, false, new ArrayList<>());
    }

    public Scn executePendingReplays(List<LogFile> archiveLogs,
                                     EventDispatcher<OraclePartition, TableId> dispatcher,
                                     OraclePartition partition,
                                     OracleOffsetContext offsetContext,
                                     ZoneOffset databaseOffset)
            throws InterruptedException {

        final List<LogFile> sortedLogs = archiveLogs.stream()
                .sorted(Comparator.comparing(LogFile::getFirstScn))
                .toList();

        if (pendingReplayUnits.isEmpty()) {
            return Scn.NULL;
        }

        final List<WorkUnit> replayUnits = List.copyOf(pendingReplayUnits);

        LOGGER.info("Starting replay follow-up wave: logs={}, workUnits={}, pendingDispatch={}",
                describeLogs(sortedLogs), replayUnits.size(), pendingDispatch.size());
        planner.logPlannedWorkUnits(replayUnits);

        final List<WorkerResult> results = executeWorkers(replayUnits);
        pendingReplayUnits.clear();

        LOGGER.info("Replay follow-up worker execution complete: results={}", describeResults(results));
        return merger.mergeAndDispatch(sortedLogs, results, dispatcher, partition, offsetContext, databaseOffset, false, new ArrayList<>());
    }

    @Override
    public void close() {
        executorService.shutdownNow();
        try {
            if (!executorService.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                LOGGER.warn("LogMiner worker thread pool did not terminate within {} seconds", EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------ protected hooks (overridable for testing)

    /**
     * Submits the given work units to the thread pool and collects their results.
     * Overriding this method in tests allows injecting pre-canned {@link WorkerResult} objects
     * without requiring a live Oracle database connection.
     *
     * @param units work units to execute
     * @return results in the same order as the units list
     * @throws InterruptedException if the thread is interrupted while waiting for a worker
     */
    protected List<WorkerResult> executeWorkers(List<WorkUnit> units) throws InterruptedException {
        final List<Future<WorkerResult>> futures = new ArrayList<>(units.size());
        for (int i = 0; i < units.size(); i++) {
            final WorkUnit unit = units.get(i);
            futures.add(executorService.submit(
                    new LogMinerWorker(i, unit, connectorConfig, schema, jdbcConfig)));
        }
        final List<WorkerResult> results = new ArrayList<>(futures.size());
        for (Future<WorkerResult> future : futures) {
            try {
                results.add(future.get());
            }
            catch (ExecutionException e) {
                final Throwable cause = e.getCause();
                LOGGER.error("LogMiner worker failed", cause);
                if (cause instanceof RuntimeException rte) {
                    throw rte;
                }
                throw new RuntimeException("LogMiner worker failed", cause);
            }
        }
        return results;
    }

    /**
     * Submits the given work units and collects their results in completion order.
        * Archive waves use this to observe completion order for diagnostics while deferring merge
        * and dispatch until the full wave has completed.
     */
    protected void executeWorkersAsCompleted(List<WorkUnit> units, WorkerResultConsumer consumer) throws InterruptedException {
        final ExecutorCompletionService<WorkerResult> completionService = new ExecutorCompletionService<>(executorService);
        for (int i = 0; i < units.size(); i++) {
            completionService.submit(new LogMinerWorker(i, units.get(i), connectorConfig, schema, jdbcConfig));
        }

        for (int i = 0; i < units.size(); i++) {
            try {
                consumer.accept(completionService.take().get());
            }
            catch (ExecutionException e) {
                final Throwable cause = e.getCause();
                LOGGER.error("LogMiner worker failed", cause);
                if (cause instanceof RuntimeException rte) {
                    throw rte;
                }
                throw new RuntimeException("LogMiner worker failed", cause);
            }
        }
    }

    protected Scn mergeCompletedWorkerPrefix(List<LogFile> availableLogs,
                                             List<WorkerResult> results,
                                             EventDispatcher<OraclePartition, TableId> dispatcher,
                                             OraclePartition partition,
                                             OracleOffsetContext offsetContext,
                                             ZoneOffset databaseOffset,
                                             boolean updatePendingReplayUnits,
                                             List<WorkerResult.SchemaChangeRecord> appliedWaveSchemaChanges)
            throws InterruptedException {
        return merger.mergeAndDispatch(
                availableLogs,
                results,
                dispatcher,
                partition,
                offsetContext,
                databaseOffset,
                updatePendingReplayUnits,
                appliedWaveSchemaChanges);
    }

    /**
     * Exposes work-unit planning for testing purposes.
     * @see #buildWorkUnits(List, Scn)
     */
    protected List<WorkUnit> planWorkUnits(List<LogFile> sortedLogs) {
        return planner.buildWorkUnits(sortedLogs, sortedLogs.get(0).getFirstScn(), pendingInheritedTransactions, pendingOrphanCommits);
    }

    protected List<WorkUnit> planWorkUnits(List<LogFile> sortedLogs, Scn readStartScn) {
        return planner.buildWorkUnits(sortedLogs, readStartScn, pendingInheritedTransactions, pendingOrphanCommits);
    }

    // ------------------------------------------------------------------ private helpers



    private String describeResults(List<WorkerResult> results) {
        return results.stream()
                .map(result -> "worker=" + result.workerId()
                        + ":type=" + result.unitType()
                        + ",resolved=" + result.resolvedTransactions().size()
                        + ",unresolved=" + result.unresolvedTransactions().size()
                        + ",orphans=" + result.orphanCommits().size()
                        + ",schemaChanges=" + result.schemaChanges().size()
                        + ",finalReadScn=" + result.finalReadScn())
                .toList()
                .toString();
    }


}
