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
        final List<WorkUnit> units = buildWorkUnits(sortedLogs, readStartScn);

        LOGGER.info("Starting concurrent wave: logs={}, workUnits={}, workersConfigured={}, pendingInherited={}, pendingOrphans={}, pendingDispatch={}",
                describeLogs(sortedLogs), units.size(), concurrentReaders,
                savedInherited.size(), savedOrphans.size(), pendingDispatch.size());
        logPlannedWorkUnits(units);

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

            final List<WorkUnit> knownCommitReplayUnits = planReplayWorkUnits(sortedLogs, results, planKnownCommitContinuations());
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

        final WorkUnit unit = buildSerialWorkUnit(allLogs, readStartScn, readEndScn);

        LOGGER.info("Starting serial wave: logs={}, session=[{},{}], inheritedTransactions={}",
                describeLogs(unit.logFiles()), unit.sessionStartScn(), unit.sessionEndScn(),
                describeTransactionIds(pendingInheritedTransactions.stream()
                        .map(InheritedTransaction::transactionId)
                        .toList()));

        try {
            dispatchPendingBeforeSerial(dispatcher, partition, offsetContext, databaseOffset);
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
                        dispatchCommittedTransaction(transaction, dispatcher, partition, offsetContext, schema, databaseOffset);
                    },
                    change -> applySchemaChange(change, dispatcher, partition, offsetContext, schema))
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
        updateCrossWaveState(result);

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

    WorkUnit buildSerialWorkUnit(List<LogFile> allLogs, Scn readStartScn, Scn readEndScn) {
        final List<LogFile> sortedLogs = allLogs.stream()
                .sorted(Comparator.comparing(LogFile::getFirstScn))
                .toList();

        final Scn sessionStartScn = sortedLogs.stream()
                .map(LogFile::getFirstScn)
                .min(Comparator.naturalOrder())
                .orElse(Scn.NULL);

        // Keep the serial worker on the same bounded batch window as the main buffered flow.
        final Scn sessionEndScn = readEndScn.isNull()
                ? sortedLogs.stream().map(LogFile::getNextScn).max(Comparator.naturalOrder()).orElse(Scn.NULL)
                : readEndScn;

        return new WorkUnit(
                WorkUnitType.WORKER,
                sortedLogs,
                sessionStartScn,
                sessionEndScn,
                readStartScn,
                sessionEndScn,
                List.copyOf(pendingInheritedTransactions));
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
        logPlannedWorkUnits(units);

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
            dispatchCommittedTransaction(tx, dispatcher, partition, offsetContext, schema, databaseOffset);
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

        final List<WorkUnit> bridgeUnits = buildBridgeUnits(sortedLogs);
        if (bridgeUnits.isEmpty()) {
            return Scn.NULL;
        }

        LOGGER.info("Starting bridge follow-up wave: logs={}, workUnits={}, pendingInherited={}, pendingOrphans={}, pendingDispatch={}",
                describeLogs(sortedLogs), bridgeUnits.size(),
                savedInherited.size(), savedOrphans.size(), pendingDispatch.size());
        logPlannedWorkUnits(bridgeUnits);

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
        return mergeAndDispatch(sortedLogs, results, dispatcher, partition, offsetContext, databaseOffset, false, new ArrayList<>());
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
        logPlannedWorkUnits(replayUnits);

        final List<WorkerResult> results = executeWorkers(replayUnits);
        pendingReplayUnits.clear();

        LOGGER.info("Replay follow-up worker execution complete: results={}", describeResults(results));
        return mergeAndDispatch(sortedLogs, results, dispatcher, partition, offsetContext, databaseOffset, false, new ArrayList<>());
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
        return mergeAndDispatch(
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
        if (sortedLogs.isEmpty()) {
            return List.of();
        }
        return buildWorkUnits(sortedLogs, sortedLogs.get(0).getFirstScn());
    }

    protected List<WorkUnit> planWorkUnits(List<LogFile> sortedLogs, Scn readStartScn) {
        return buildWorkUnits(sortedLogs, readStartScn);
    }

    // ------------------------------------------------------------------ private helpers

    /**
     * Partitions the sorted archive log list into work units.
     *
     * <p>WORKER units are created by distributing the logs across {@code concurrentReaders}.
     * Any pending BRIDGE units (from previous-wave unresolved transactions) are prepended so they
     * run concurrently with the new WORKER units.
     */
    private List<WorkUnit> buildWorkUnits(List<LogFile> sortedLogs, Scn readStartScn) {
        final List<WorkUnit> units = new ArrayList<>();

        // First, try to match pending orphan commits against pending inherited transactions and
        // create BRIDGE units for exact matches.
        final List<WorkUnit> bridgeUnits = buildBridgeUnits(sortedLogs);
        units.addAll(bridgeUnits);

        // WORKER units for the clean log slices in this wave.
        // Skip logs that are already covered by BRIDGE units.
        final List<LogFile> workerLogs = getWorkerLogs(sortedLogs, bridgeUnits);
        if (!workerLogs.isEmpty()) {
            units.addAll(buildWorkerUnits(workerLogs, readStartScn));
        }

        return units;
    }

    private void logPlannedWorkUnits(List<WorkUnit> units) {
        for (int i = 0; i < units.size(); i++) {
            final WorkUnit unit = units.get(i);
            LOGGER.info("Planned work unit {}: type={}, session=[{},{}], read=[{},{}], logs={}, inheritedTransactions={}",
                    i,
                    unit.type(),
                    unit.sessionStartScn(),
                    unit.sessionEndScn(),
                    unit.readStartScn(),
                    unit.readEndScn(),
                    describeLogs(unit.logFiles()),
                    describeTransactionIds(unit.inheritedTransactions().stream()
                            .map(InheritedTransaction::transactionId)
                            .toList()));
        }
    }

    protected List<KnownCommitContinuationPlan> planKnownCommitContinuations(List<LogFile> sortedLogs) {
        final List<KnownCommitContinuationPlan> plans = new ArrayList<>();

        for (OrphanCommit orphan : pendingOrphanCommits) {
            final InheritedTransaction inherited = pendingInheritedTransactions.stream()
                    .filter(t -> t.transactionId().equals(orphan.transactionId()))
                    .findFirst()
                    .orElse(null);
            if (inherited == null) {
                continue;
            }

            final Scn sessionStartScn = inherited.startScn().subtract(Scn.ONE);
            final List<LogFile> continuationLogs = sortedLogs.stream()
                    .filter(log -> !log.getNextScn().isNull()
                            && log.getNextScn().compareTo(sessionStartScn) > 0
                            && log.getFirstScn().compareTo(orphan.commitScn()) <= 0)
                    .sorted(Comparator.comparing(LogFile::getFirstScn))
                    .toList();

            if (continuationLogs.isEmpty()) {
                LOGGER.warn("No logs available to build BRIDGE unit for transaction {} (start={}, commit={})",
                        orphan.transactionId(), inherited.startScn(), orphan.commitScn());
                continue;
            }

            plans.add(new KnownCommitContinuationPlan(
                    inherited,
                    orphan,
                    continuationLogs,
                    sessionStartScn,
                    inherited.trustedPrefixScn(),
                    orphan.commitScn()));
        }

        return plans;
    }

    protected List<KnownCommitContinuationPlan> planKnownCommitContinuations() {
        final List<KnownCommitContinuationPlan> plans = new ArrayList<>();

        for (OrphanCommit orphan : pendingOrphanCommits) {
            final InheritedTransaction inherited = pendingInheritedTransactions.stream()
                    .filter(t -> t.transactionId().equals(orphan.transactionId()))
                    .findFirst()
                    .orElse(null);
            if (inherited == null) {
                continue;
            }

            plans.add(new KnownCommitContinuationPlan(
                    inherited,
                    orphan,
                    List.of(),
                    inherited.startScn().subtract(Scn.ONE),
                    inherited.trustedPrefixScn(),
                    orphan.commitScn()));
        }

        return plans;
    }

    protected List<WorkerResult> findOverlappingWorkerResults(List<WorkerResult> results,
                                                              List<KnownCommitContinuationPlan> continuationPlans) {
        if (results.isEmpty() || continuationPlans.isEmpty()) {
            return List.of();
        }

        return results.stream()
                .filter(result -> result.unitType() == WorkUnitType.WORKER)
                .filter(result -> continuationPlans.stream().anyMatch(plan -> overlapsUnsafeInterval(result, plan)))
                .toList();
    }

    protected List<WorkUnit> planReplayWorkUnits(List<LogFile> sortedLogs,
                                                 List<WorkerResult> results,
                                                 List<KnownCommitContinuationPlan> continuationPlans) {
        final List<WorkUnit> replayUnits = new ArrayList<>();
        if (sortedLogs.isEmpty() || results.isEmpty() || continuationPlans.isEmpty()) {
            return replayUnits;
        }

        for (WorkerResult result : findOverlappingWorkerResults(results, continuationPlans)) {
            final Scn replayStartScn = continuationPlans.stream()
                    .filter(plan -> overlapsUnsafeInterval(result, plan))
                    .map(KnownCommitContinuationPlan::unsafeIntervalEndScn)
                    .max(Comparator.naturalOrder())
                    .orElse(Scn.NULL);

            if (replayStartScn.isNull() || result.finalReadScn().compareTo(replayStartScn) <= 0) {
                continue;
            }

            final List<LogFile> replayLogs = sortedLogs.stream()
                    .filter(log -> overlapsReplayTail(log, replayStartScn, result.finalReadScn()))
                    .toList();

            if (replayLogs.isEmpty()) {
                continue;
            }

            final Scn sessionStartScn = replayLogs.get(0).getFirstScn();
            replayUnits.add(new WorkUnit(
                    WorkUnitType.WORKER,
                    replayLogs,
                    sessionStartScn,
                    result.finalReadScn(),
                    replayStartScn,
                    result.finalReadScn(),
                    List.of()));
        }

        return replayUnits;
    }

    /**
     * Builds BRIDGE units for pending orphan commits that have matching pending inherited
     * transactions.
     *
     * <p>Compatible matches are batched into a single BRIDGE unit so one worker can resolve
     * multiple cross-log transactions in one pass over the shared unread window. The bridge
         * session still extends back to cover the earliest transaction START (Golden Rule), while
         * the read window is the explicitly planned unsafe interval derived from the trusted prefix
         * and the known commit SCN.
     */
    private List<WorkUnit> buildBridgeUnits(List<LogFile> sortedLogs) {
        final List<WorkUnit> bridges = new ArrayList<>();

        final Map<String, BridgeBatch> batches = new LinkedHashMap<>();
        final List<OrphanCommit> matchedOrphans = new ArrayList<>();
        final List<InheritedTransaction> matchedInherited = new ArrayList<>();

        for (KnownCommitContinuationPlan plan : planKnownCommitContinuations(sortedLogs)) {
            // Batch compatible bridge pairs so one worker can resolve multiple transactions
            // in a single pass over the shared unread window.
            final String batchKey = plan.unsafeIntervalStartScn() + "|" + plan.logFiles().stream()
                    .map(LogFile::getFileName)
                    .reduce((left, right) -> left + "," + right)
                    .orElse("");

            final BridgeBatch batch = batches.computeIfAbsent(batchKey,
                    key -> new BridgeBatch(plan.logFiles(), plan.unsafeIntervalStartScn()));
            batch.add(plan);
            matchedOrphans.add(plan.orphanCommit());
            matchedInherited.add(plan.inheritedTransaction());
        }

        for (BridgeBatch batch : batches.values()) {
            bridges.add(new WorkUnit(
                    WorkUnitType.BRIDGE,
                    batch.logFiles,
                    batch.earliestStartScn.subtract(Scn.ONE),
                    batch.latestCommitScn,
                    batch.unsafeIntervalStartScn,
                    batch.latestCommitScn,
                    List.copyOf(batch.inheritedTransactions)));
            LOGGER.info("Planned BRIDGE batch: transactions={}, unsafeInterval=({},{}], logs={}",
                    describeTransactionIds(batch.inheritedTransactions.stream()
                            .map(InheritedTransaction::transactionId)
                            .toList()),
                    batch.unsafeIntervalStartScn,
                    batch.latestCommitScn,
                    describeLogs(batch.logFiles));
        }

        pendingOrphanCommits.removeAll(matchedOrphans);
        pendingInheritedTransactions.removeAll(matchedInherited);

        return bridges;
    }

    private boolean overlapsUnsafeInterval(WorkerResult result, KnownCommitContinuationPlan plan) {
        return result.finalReadScn().compareTo(plan.unsafeIntervalStartScn()) > 0
                && result.readStartScn().compareTo(plan.unsafeIntervalEndScn()) < 0;
    }

    private List<CommittedTransaction> filterResolvedTransactions(WorkerResult result,
                                                                  List<KnownCommitContinuationPlan> continuationPlans,
                                                                  Set<Integer> overlappingWorkerIds) {
        if (result.unitType() != WorkUnitType.WORKER || !overlappingWorkerIds.contains(result.workerId())) {
            return result.resolvedTransactions();
        }

        final List<KnownCommitContinuationPlan> overlappingPlans = continuationPlans.stream()
                .filter(plan -> overlapsUnsafeInterval(result, plan))
                .toList();

        // For overlapping workers, dispatch transactions that are either fully inside the
        // trusted prefix or that straddle the bridge/replay boundary and therefore cannot be
        // reconstructed by the bridge or replay follow-up units.
        final Scn trustedCutoff = overlappingPlans.stream()
                .map(KnownCommitContinuationPlan::unsafeIntervalStartScn)
                .min(Comparator.naturalOrder())
                .orElse(null);
        final Scn replayStartScn = overlappingPlans.stream()
                .map(KnownCommitContinuationPlan::unsafeIntervalEndScn)
                .max(Comparator.naturalOrder())
                .orElse(null);

        if (trustedCutoff == null || replayStartScn == null) {
            return result.resolvedTransactions();
        }

        final Scn cutoff = trustedCutoff;
        final Scn replayStart = replayStartScn;
        return result.resolvedTransactions().stream()
                .filter(tx -> tx.commitScn().compareTo(cutoff) <= 0
                        || tx.commitScn().compareTo(replayStart) > 0)
                .toList();
    }

    private List<WorkerResult.SchemaChangeRecord> filterSchemaChanges(WorkerResult result,
                                                                      List<KnownCommitContinuationPlan> continuationPlans,
                                                                      Set<Integer> overlappingWorkerIds) {
        if (result.unitType() != WorkUnitType.WORKER || !overlappingWorkerIds.contains(result.workerId())) {
            return result.schemaChanges();
        }

        final List<KnownCommitContinuationPlan> overlappingPlans = continuationPlans.stream()
                .filter(plan -> overlapsUnsafeInterval(result, plan))
                .toList();

        final Scn trustedCutoff = overlappingPlans.stream()
                .map(KnownCommitContinuationPlan::unsafeIntervalStartScn)
                .min(Comparator.naturalOrder())
                .orElse(null);
        final Scn replayStartScn = overlappingPlans.stream()
                .map(KnownCommitContinuationPlan::unsafeIntervalEndScn)
                .max(Comparator.naturalOrder())
                .orElse(null);

        if (trustedCutoff == null || replayStartScn == null) {
            return result.schemaChanges();
        }

        final Scn cutoff = trustedCutoff;
        final Scn replayStart = replayStartScn;
        return result.schemaChanges().stream()
                .filter(change -> change.scn().compareTo(cutoff) <= 0
                        || change.scn().compareTo(replayStart) > 0)
                .toList();
    }

    private static final class BridgeBatch {
        private final List<LogFile> logFiles;
        private final Scn unsafeIntervalStartScn;
        private final List<InheritedTransaction> inheritedTransactions = new ArrayList<>();
        private Scn earliestStartScn;
        private Scn latestCommitScn;

        private BridgeBatch(List<LogFile> logFiles, Scn unsafeIntervalStartScn) {
            this.logFiles = logFiles;
            this.unsafeIntervalStartScn = unsafeIntervalStartScn;
        }

        private void add(KnownCommitContinuationPlan plan) {
            final InheritedTransaction inherited = plan.inheritedTransaction();
            final OrphanCommit orphan = plan.orphanCommit();

            inheritedTransactions.add(inherited);
            if (earliestStartScn == null || inherited.startScn().compareTo(earliestStartScn) < 0) {
                earliestStartScn = inherited.startScn();
            }
            if (latestCommitScn == null || orphan.commitScn().compareTo(latestCommitScn) > 0) {
                latestCommitScn = orphan.commitScn();
            }
        }
    }

    static record KnownCommitContinuationPlan(
            InheritedTransaction inheritedTransaction,
            OrphanCommit orphanCommit,
            List<LogFile> logFiles,
            Scn sessionStartScn,
            Scn unsafeIntervalStartScn,
            Scn unsafeIntervalEndScn) {
    }

    /**
     * Returns the logs whose read slices do not overlap any BRIDGE unsafe interval.
     *
     * <p>A bridge may register earlier logs purely to satisfy the Golden Rule for session start.
     * Those session-only logs should remain eligible for normal WORKER processing when their own
     * read slices lie entirely outside the bridge read window.
     */
    private List<LogFile> getWorkerLogs(List<LogFile> sortedLogs,
                                        List<WorkUnit> bridgeUnits) {
        return sortedLogs.stream()
                .filter(log -> bridgeUnits.stream().noneMatch(unit -> overlapsBridgeUnsafeInterval(log, unit)))
                .toList();
    }

    private boolean overlapsBridgeUnsafeInterval(LogFile log, WorkUnit bridgeUnit) {
        return !log.getNextScn().isNull()
                && log.getNextScn().compareTo(bridgeUnit.readStartScn()) > 0
                && log.getFirstScn().compareTo(bridgeUnit.readEndScn()) <= 0;
    }

    private boolean overlapsReplayTail(LogFile log, Scn replayStartScn, Scn replayEndScn) {
        return !log.getNextScn().isNull()
                && log.getNextScn().compareTo(replayStartScn) > 0
                && log.getFirstScn().compareTo(replayEndScn) <= 0;
    }

    /**
     * Distributes the worker logs contiguously across {@code concurrentReaders} work units.
     *
     * <p>Contiguous partitioning keeps each worker's LogMiner session window minimal:
     * logs are assigned in order so that a worker's session covers a single continuous
     * SCN range, avoiding wasted database work on gaps between non-adjacent logs.
     */
    private List<WorkUnit> buildWorkerUnits(List<LogFile> workerLogs, Scn readStartScn) {
        final List<WorkUnit> units = new ArrayList<>();
        final int readers = Math.min(concurrentReaders, workerLogs.size());
        // Contiguous partition: each reader gets ceiling(n/readers) consecutive logs
        final int chunkSize = workerLogs.size() / readers;
        final int remainder = workerLogs.size() % readers;
        int start = 0;
        for (int i = 0; i < readers; i++) {
            final int endExclusive = start + chunkSize + (i < remainder ? 1 : 0);
            final List<LogFile> partition = new ArrayList<>(workerLogs.subList(start, endExclusive));
            start = endExclusive;

            final Scn firstScn = partition.get(0).getFirstScn();
            final Scn nextScn = partition.get(partition.size() - 1).getNextScn();
            final Scn workerReadStartScn = i == 0 ? readStartScn : firstScn;
            units.add(WorkUnit.worker(partition, firstScn, nextScn, workerReadStartScn));
        }

        return units;
    }

    /**
     * Merges all worker results, applies schema changes, dispatches committed transactions in strict
     * global SCN order, and updates cross-wave pending state.
     *
     * <h3>Ordering guarantee</h3>
     * <p>Within a wave, all resolved transactions are dispatched in {@code commitScn} order
     * (tie-broken by {@code commitRsId} to match LogMiner's {@code ORDER BY scn, rs_id}).
     *
    * <p>Cross-log transactions (START in one worker's slice, COMMIT in another's) are not
    * fully resolved in this merge pass. They remain pending until a BRIDGE unit runs in a later
    * wave, but their {@code commitScn} may
     * fall <em>below</em> transactions already dispatched in the current wave. To prevent this
     * reordering, a <em>safe dispatch horizon</em> is computed from the minimum commitScn of
     * any cross-log transaction detected in this wave (or {@code lastReadScn} for those whose
     * commitScn is still unknown). Any resolved transaction with {@code commitScn >= horizon}
     * is held in {@link #pendingDispatch} and prepended to the next wave's sort, ensuring that
     * when the BRIDGE transaction is finally resolved it slots into its correct position.
     *
     * @return the highest SCN that was fully processed in this wave
     */
    private Scn mergeAndDispatch(
                                 List<LogFile> availableLogs,
                                 List<WorkerResult> results,
                                 EventDispatcher<OraclePartition, TableId> dispatcher,
                                 OraclePartition partition,
                                 OracleOffsetContext offsetContext,
                                 ZoneOffset databaseOffset,
                                 boolean updatePendingReplayUnits,
                                 List<WorkerResult.SchemaChangeRecord> appliedWaveSchemaChanges)
            throws InterruptedException {

        // Start with any transactions held back from previous waves
        final List<CommittedTransaction> allResolved = new ArrayList<>(pendingDispatch);
        pendingDispatch.clear();

        // Prepend schema changes held back above the safe dispatch horizon in the prior wave.
        final List<WorkerResult.SchemaChangeRecord> allSchemaChanges = new ArrayList<>(pendingSchemaChanges);
        pendingSchemaChanges.clear();
        Scn maxProcessedScn = Scn.NULL;

        for (WorkerResult result : results) {
            if (maxProcessedScn.isNull() || result.finalReadScn().compareTo(maxProcessedScn) > 0) {
                maxProcessedScn = result.finalReadScn();
            }

            // Accumulate cross-wave state; must run before computeSafeDispatchHorizon()
            updateCrossWaveState(result);
        }

        pruneResolvedCrossWaveState(results);

        final List<KnownCommitContinuationPlan> continuationPlans = planKnownCommitContinuations();
        final Set<Integer> overlappingWorkerIds = findOverlappingWorkerResults(results, continuationPlans).stream()
                .map(WorkerResult::workerId)
                .collect(Collectors.toSet());

        if (updatePendingReplayUnits) {
            pendingReplayUnits.clear();
            pendingReplayUnits.addAll(planReplayWorkUnits(availableLogs, results, continuationPlans));
        }

        allResolved.removeIf(tx -> isRecoverableByKnownCommitContinuation(tx, continuationPlans));
        allSchemaChanges.removeIf(change -> isRecoverableByKnownCommitContinuation(change, continuationPlans));

        for (WorkerResult result : results) {
            allResolved.addAll(filterResolvedTransactions(result, continuationPlans, overlappingWorkerIds));
            allSchemaChanges.addAll(filterSchemaChanges(result, continuationPlans, overlappingWorkerIds));
        }

        deduplicateResolvedTransactions(allResolved);

        final List<WorkerResult.SchemaChangeRecord> contaminationSchemaChanges = new ArrayList<>(appliedWaveSchemaChanges);
        contaminationSchemaChanges.addAll(allSchemaChanges);

        // Sort by commitScn, then by commitRsId (preserves LogMiner's rs_id tie-breaking order)
        allResolved.sort(Comparator.comparing(CommittedTransaction::commitScn)
                .thenComparing(CommittedTransaction::commitRsId,
                        Comparator.nullsLast(Comparator.naturalOrder())));
        allSchemaChanges.sort(Comparator.comparing(WorkerResult.SchemaChangeRecord::scn));

        // Quarantine any resolved transactions whose DML events were parsed against a stale
        // schema because a DDL occurred for the same table at a lower SCN in this wave.
        // Workers run concurrently before the coordinator applies any schema changes, so DML
        // events after the DDL use the pre-DDL column layout. The quarantined transactions are
        // scheduled for a targeted replay after the schema update is applied.
        if (!contaminationSchemaChanges.isEmpty()) {
            final List<CommittedTransaction> contaminated = detectDdlContaminatedTransactions(allResolved, contaminationSchemaChanges);
            if (!contaminated.isEmpty()) {
                LOGGER.warn("Detected {} DDL-contaminated transaction(s) whose DML was parsed against a stale schema; "
                        + "quarantining for replay after schema update: {}",
                        contaminated.size(),
                        describeTransactionIds(contaminated.stream().map(CommittedTransaction::transactionId).toList()));
                allResolved.removeAll(contaminated);
                scheduleContaminatedTransactionReplay(availableLogs, contaminated, contaminationSchemaChanges);
            }
        }

        // Compute safe dispatch horizon: the lowest SCN at which a cross-log transaction
        // could commit. Transactions at or above this SCN are held back for the next wave.
        final Scn safeHorizon = computeSafeDispatchHorizon();
        if (safeHorizon != null) {
            LOGGER.info("Safe dispatch horizon active: {}. Transactions at or above this SCN stay pending.", safeHorizon);
        }

        // Interleave and dispatch
        final Iterator<WorkerResult.SchemaChangeRecord> schemaIter = allSchemaChanges.iterator();
        WorkerResult.SchemaChangeRecord pendingSchemaChange = schemaIter.hasNext() ? schemaIter.next() : null;
        final List<WorkerResult.SchemaChangeRecord> newlyAppliedSchemaChanges = new ArrayList<>();

        for (CommittedTransaction tx : allResolved) {
            // Hold back transactions at or above the safe horizon
            if (safeHorizon != null && tx.commitScn().compareTo(safeHorizon) >= 0) {
                pendingDispatch.add(tx);
                continue;
            }

            // Apply any schema changes that precede this transaction's commitScn
            while (pendingSchemaChange != null &&
                    pendingSchemaChange.scn().compareTo(tx.commitScn()) <= 0) {
                applySchemaChange(pendingSchemaChange, dispatcher, partition, offsetContext, schema);
                newlyAppliedSchemaChanges.add(pendingSchemaChange);
                pendingSchemaChange = schemaIter.hasNext() ? schemaIter.next() : null;
            }

            dispatchCommittedTransaction(tx, dispatcher, partition, offsetContext, schema, databaseOffset);
        }

        // Flush remaining schema changes below the safe dispatch horizon.
        // Schema changes at or above the horizon are carried forward to the next wave so that
        // they are applied in the correct SCN order when the held-back transactions are dispatched.
        while (pendingSchemaChange != null) {
            if (safeHorizon != null && pendingSchemaChange.scn().compareTo(safeHorizon) >= 0) {
                pendingSchemaChanges.add(pendingSchemaChange);
                schemaIter.forEachRemaining(pendingSchemaChanges::add);
                break;
            }
            applySchemaChange(pendingSchemaChange, dispatcher, partition, offsetContext, schema);
            newlyAppliedSchemaChanges.add(pendingSchemaChange);
            pendingSchemaChange = schemaIter.hasNext() ? schemaIter.next() : null;
        }

        appliedWaveSchemaChanges.addAll(newlyAppliedSchemaChanges);

        if (!maxProcessedScn.isNull()) {
            offsetContext.setScn(maxProcessedScn);
            dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
        }

        LOGGER.info("Concurrent wave merge complete: dispatched={}, heldBack={}, schemaChanges={}, pendingInherited={}, pendingOrphans={}, nextScn={}",
                allResolved.size() - pendingDispatch.size(),
                pendingDispatch.size(),
                allSchemaChanges.size(),
                pendingInheritedTransactions.size(),
                pendingOrphanCommits.size(),
                maxProcessedScn);

        return maxProcessedScn;
    }

    /**
     * Computes the safe dispatch horizon: the lowest SCN at or above which resolved transactions
     * must be withheld this wave because a cross-log transaction could commit there.
     *
     * <ul>
     *   <li>For each pending unresolved transaction that now has a matching orphan commit, the
     *       horizon candidate is {@code orphan.commitScn}.</li>
    *   <li>For unresolved transactions with no known commit SCN, the candidate
     *       is {@code unresolved.lastReadScn} ╬ô├ç├╢ we know the commit is somewhere above it, so
     *       dispatching anything at or above {@code lastReadScn} could violate order.</li>
     * </ul>
     *
     * <p>Returns {@code null} if there are no pending unresolved transactions, meaning it is safe
     * to dispatch everything.
     */
    private Scn computeSafeDispatchHorizon() {
        if (pendingInheritedTransactions.isEmpty()) {
            return null; // nothing unresolved ╬ô├ç├╢ dispatch everything
        }

        // Build a quick lookup of orphan commit SCNs by transaction id
        final Map<String, Scn> orphanScnByTxId = new HashMap<>();
        for (OrphanCommit orphan : pendingOrphanCommits) {
            orphanScnByTxId.put(orphan.transactionId(), orphan.commitScn());
        }

        Scn horizon = null;
        for (InheritedTransaction unresolved : pendingInheritedTransactions) {
            final Scn candidate;
            final Scn matchedCommitScn = orphanScnByTxId.get(unresolved.transactionId());
            if (matchedCommitScn != null) {
                // We know exactly where this transaction commits ╬ô├ç├╢ use that as the horizon
                candidate = matchedCommitScn;
            }
            else {
                // Unknown commit SCN; any SCN above lastReadScn could be this transaction's commit.
                // Conservatively hold back everything at or above lastReadScn.
                candidate = unresolved.lastReadScn();
            }

            if (horizon == null || candidate.compareTo(horizon) < 0) {
                horizon = candidate;
            }
        }
        return horizon;
    }

    private boolean isRecoverableByKnownCommitContinuation(CommittedTransaction transaction,
                                                           List<KnownCommitContinuationPlan> continuationPlans) {
        return continuationPlans.stream().anyMatch(plan -> transaction.commitScn().compareTo(plan.unsafeIntervalStartScn()) > 0
                && transaction.commitScn().compareTo(plan.unsafeIntervalEndScn()) <= 0);
    }

    private boolean isRecoverableByKnownCommitContinuation(WorkerResult.SchemaChangeRecord schemaChange,
                                                           List<KnownCommitContinuationPlan> continuationPlans) {
        return continuationPlans.stream().anyMatch(plan -> schemaChange.scn().compareTo(plan.unsafeIntervalStartScn()) > 0
                && schemaChange.scn().compareTo(plan.unsafeIntervalEndScn()) <= 0);
    }

    private void deduplicateResolvedTransactions(List<CommittedTransaction> resolvedTransactions) {
        if (resolvedTransactions.size() < 2) {
            return;
        }

        final Map<String, CommittedTransaction> latestByTransactionId = new LinkedHashMap<>();
        for (CommittedTransaction transaction : resolvedTransactions) {
            latestByTransactionId.put(transaction.transactionId(), transaction);
        }

        if (latestByTransactionId.size() == resolvedTransactions.size()) {
            return;
        }

        resolvedTransactions.clear();
        resolvedTransactions.addAll(latestByTransactionId.values());
    }

    /**
     * Updates the coordinator's cross-wave pending state with new unresolved and orphan data
     * returned by the given result.
     */
    private void updateCrossWaveState(WorkerResult result) {
        // Add new unresolved transactions (de-duplicate by transactionId)
        final Map<String, InheritedTransaction> existingById = new HashMap<>();
        for (InheritedTransaction t : pendingInheritedTransactions) {
            existingById.put(t.transactionId(), t);
        }
        for (InheritedTransaction newTx : result.unresolvedTransactions()) {
            existingById.put(newTx.transactionId(), newTx); // newer entry overwrites (has more events)
        }
        pendingInheritedTransactions.clear();
        pendingInheritedTransactions.addAll(existingById.values());

        // Add new orphan commits (de-duplicate by transactionId + commitScn)
        final Set<OrphanCommitKey> knownOrphans = pendingOrphanCommits.stream()
                .map(orphan -> new OrphanCommitKey(orphan.transactionId(), orphan.commitScn()))
                .collect(Collectors.toCollection(HashSet::new));
        for (OrphanCommit orphan : result.orphanCommits()) {
            if (knownOrphans.add(new OrphanCommitKey(orphan.transactionId(), orphan.commitScn()))) {
                pendingOrphanCommits.add(orphan);
            }
        }

        LOGGER.info("Updated cross-wave state from worker {}: unresolvedNow={}, orphansNow={}, unresolvedTransactions={}, orphanTransactions={}",
                result.workerId(),
                pendingInheritedTransactions.size(),
                pendingOrphanCommits.size(),
                describeTransactionIds(pendingInheritedTransactions.stream()
                        .map(InheritedTransaction::transactionId)
                        .toList()),
                describeTransactionIds(pendingOrphanCommits.stream()
                        .map(OrphanCommit::transactionId)
                        .toList()));
    }

    private void dispatchPendingBeforeSerial(EventDispatcher<OraclePartition, TableId> dispatcher,
                                             OraclePartition partition,
                                             OracleOffsetContext offsetContext,
                                             ZoneOffset databaseOffset)
            throws InterruptedException {

        if (pendingDispatch.isEmpty() && pendingSchemaChanges.isEmpty()) {
            return;
        }

        final List<CommittedTransaction> heldBackTransactions = new ArrayList<>(pendingDispatch);
        final List<WorkerResult.SchemaChangeRecord> heldBackSchemaChanges = new ArrayList<>(pendingSchemaChanges);
        pendingDispatch.clear();
        pendingSchemaChanges.clear();

        heldBackTransactions.sort(Comparator.comparing(CommittedTransaction::commitScn)
                .thenComparing(CommittedTransaction::commitRsId,
                        Comparator.nullsLast(Comparator.naturalOrder())));
        heldBackSchemaChanges.sort(Comparator.comparing(WorkerResult.SchemaChangeRecord::scn));

        final Iterator<WorkerResult.SchemaChangeRecord> schemaIter = heldBackSchemaChanges.iterator();
        WorkerResult.SchemaChangeRecord pendingSchemaChange = schemaIter.hasNext() ? schemaIter.next() : null;

        for (CommittedTransaction transaction : heldBackTransactions) {
            while (pendingSchemaChange != null
                    && pendingSchemaChange.scn().compareTo(transaction.commitScn()) <= 0) {
                applySchemaChange(pendingSchemaChange, dispatcher, partition, offsetContext, schema);
                pendingSchemaChange = schemaIter.hasNext() ? schemaIter.next() : null;
            }

            dispatchCommittedTransaction(transaction, dispatcher, partition, offsetContext, schema, databaseOffset);
        }

        while (pendingSchemaChange != null) {
            applySchemaChange(pendingSchemaChange, dispatcher, partition, offsetContext, schema);
            pendingSchemaChange = schemaIter.hasNext() ? schemaIter.next() : null;
        }
    }

    private record OrphanCommitKey(String transactionId, Scn commitScn) {
    }

    private void pruneResolvedCrossWaveState(List<WorkerResult> results) {
        final Set<String> resolvedTransactionIds = results.stream()
                .flatMap(result -> result.resolvedTransactions().stream())
                .map(CommittedTransaction::transactionId)
                .collect(Collectors.toSet());

        if (resolvedTransactionIds.isEmpty()) {
            return;
        }

        pendingInheritedTransactions.removeIf(tx -> resolvedTransactionIds.contains(tx.transactionId()));
        pendingOrphanCommits.removeIf(orphan -> resolvedTransactionIds.contains(orphan.transactionId()));
    }

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

    // ------------------------------- dispatch helpers -----------------------------------------------

    private void dispatchCommittedTransaction(
                                              CommittedTransaction tx,
                                              EventDispatcher<OraclePartition, TableId> dispatcher,
                                              OraclePartition partition,
                                              OracleOffsetContext offsetContext,
                                              OracleDatabaseSchema schema,
                                              ZoneOffset databaseOffset)
            throws InterruptedException {

        if (tx.events().isEmpty()) {
            offsetContext.setScn(tx.commitScn());
            dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            return;
        }

        final String transactionId = tx.transactionId();
        final Scn commitScn = tx.commitScn();

        final TransactionCommitConsumer.Handler<LogMinerEvent> delegate = (event, eventIndex, eventTrxId, eventTrxSeq, eventsProcessed) -> {
            LogMinerEventDispatchHelper.dispatchDataChangeEvent(
                    connectorConfig,
                    dispatcher,
                    partition,
                    offsetContext,
                    schema,
                    databaseOffset,
                    transactionId,
                    tx.userName(),
                    tx.redoThreadId(),
                    commitScn,
                    tx.commitTime(),
                    (DmlEvent) event,
                    eventIndex);
        };

        try (TransactionCommitConsumer commitConsumer = new TransactionCommitConsumer(delegate, connectorConfig, schema)) {
            for (LogMinerEvent event : tx.events()) {
                commitConsumer.accept(event, false, null, 0L);
            }
        }

        LogMinerEventDispatchHelper.finishCommittedTransaction(
                dispatcher,
                partition,
                offsetContext,
                tx.redoThreadId(),
                transactionId,
                commitScn,
                tx.commitRsId(),
                tx.commitTime() != null ? tx.commitTime() : Instant.now(),
                true);
    }

    private void applySchemaChange(
                                   WorkerResult.SchemaChangeRecord change,
                                   EventDispatcher<OraclePartition, TableId> dispatcher,
                                   OraclePartition partition,
                                   OracleOffsetContext offsetContext,
                                   OracleDatabaseSchema schema)
            throws InterruptedException {

        if (Strings.isNullOrBlank(change.redoSql())) {
            return;
        }

        LOGGER.debug("Applying schema change at SCN {}: {}", change.scn(), change.redoSql());
        try {
            final TableId tableId = change.tableId();
            if (tableId == null) {
                return;
            }
            offsetContext.setScn(change.scn());
            // Workers record TRUNCATE TABLE DDL rows as both a SchemaChangeRecord and a
            // TruncateEvent in the transaction cache. The coordinator applies the schema
            // update here, while the TruncateEvent is dispatched as part of the committed
            // transaction in dispatchCommittedTransaction. A no-op TruncateReceiver is
            // therefore correct: there is no buffered transaction to truncate at this point.
            final TruncateReceiver noOpTruncate = () -> {
            };
            LogMinerEventDispatchHelper.dispatchSchemaChangeEvent(
                    connectorConfig,
                    dispatcher,
                    partition,
                    offsetContext,
                    schema,
                    streamingMetrics,
                    noOpTruncate,
                    change);
        }
        catch (Exception e) {
            LOGGER.warn("Failed to apply schema change at SCN {}: {}", change.scn(), e.getMessage());
        }
    }

    /**
     * Identifies resolved transactions that contain DML events parsed against a stale schema.
     *
     * <p>A transaction is contaminated when it contains at least one DML event for table T at
     * SCN &gt; S, where S is the earliest DDL SCN for T found in {@code schemaChanges} this wave.
     * Because workers execute concurrently before the coordinator applies any schema changes,
     * those DML events were parsed using the pre-DDL column layout.
     *
     * @param resolved      all resolved transactions collected from this wave's workers
     * @param schemaChanges all schema-change records collected from this wave's workers
     * @return the subset of {@code resolved} that are contaminated (may be empty)
     */
    private List<CommittedTransaction> detectDdlContaminatedTransactions(
                                                                         List<CommittedTransaction> resolved,
                                                                         List<WorkerResult.SchemaChangeRecord> schemaChanges) {
        if (schemaChanges.isEmpty() || resolved.isEmpty()) {
            return List.of();
        }

        // Earliest DDL SCN per affected table
        final Map<TableId, Scn> earliestDdlScnByTable = new HashMap<>();
        for (final WorkerResult.SchemaChangeRecord ddl : schemaChanges) {
            if (ddl.tableId() != null) {
                earliestDdlScnByTable.merge(ddl.tableId(), ddl.scn(),
                        (existing, candidate) -> existing.compareTo(candidate) <= 0 ? existing : candidate);
            }
        }

        return resolved.stream()
                .filter(tx -> tx.events().stream()
                        .anyMatch(event -> {
                            final Scn ddlScn = earliestDdlScnByTable.get(event.getTableId());
                            return ddlScn != null && event.getScn().compareTo(ddlScn) > 0;
                        }))
                .toList();
    }

    /**
     * Schedules a targeted replay work unit for DDL-contaminated transactions and records their
     * IDs so the coordinator can filter replay results to prevent double-dispatch.
     *
     * <p>The replay covers logs from {@code minDdlScn} to {@code maxCommitScn}. Contaminated
     * transactions that already accumulated pre-DDL events (at SCN &lt; minDdlScn) are seeded as
     * {@link InheritedTransaction} records so the replay worker can attach re-parsed post-DDL
     * events to the correct accumulators without re-reading the START row.
     *
     * @param availableLogs logs available in the current wave
     * @param contaminated  resolved transactions identified as DDL-contaminated
     * @param schemaChanges schema changes from this wave (used to locate the triggering DDL SCNs)
     */
    private void scheduleContaminatedTransactionReplay(List<LogFile> availableLogs,
                                                       List<CommittedTransaction> contaminated,
                                                       List<WorkerResult.SchemaChangeRecord> schemaChanges) {
        // Build a per-table map of the earliest DDL SCN that caused contamination
        final Map<TableId, Scn> earliestDdlScnByTable = new HashMap<>();
        for (final WorkerResult.SchemaChangeRecord ddl : schemaChanges) {
            if (ddl.tableId() != null) {
                earliestDdlScnByTable.merge(ddl.tableId(), ddl.scn(),
                        (existing, candidate) -> existing.compareTo(candidate) <= 0 ? existing : candidate);
            }
        }

        // Find the minimum DDL SCN that actually caused contamination in this batch
        Scn minDdlScn = null;
        for (final CommittedTransaction tx : contaminated) {
            for (final LogMinerEvent event : tx.events()) {
                final Scn ddlScn = earliestDdlScnByTable.get(event.getTableId());
                if (ddlScn != null && event.getScn().compareTo(ddlScn) > 0) {
                    if (minDdlScn == null || ddlScn.compareTo(minDdlScn) < 0) {
                        minDdlScn = ddlScn;
                    }
                }
            }
        }

        if (minDdlScn == null) {
            return;
        }

        final Scn maxCommitScn = contaminated.stream()
                .map(CommittedTransaction::commitScn)
                .max(Comparator.naturalOrder())
                .orElse(null);

        if (maxCommitScn == null) {
            return;
        }

        // Identify logs whose SCN range overlaps [minDdlScn, maxCommitScn]
        final Scn finalMinDdlScn = minDdlScn;
        final List<LogFile> replayLogs = availableLogs.stream()
                .filter(log -> !log.getNextScn().isNull()
                        && log.getNextScn().compareTo(finalMinDdlScn) > 0
                        && log.getFirstScn().compareTo(maxCommitScn) <= 0)
                .sorted(Comparator.comparing(LogFile::getFirstScn))
                .toList();

        if (replayLogs.isEmpty()) {
            LOGGER.warn("No archive logs available to replay DDL-contaminated transactions {}; "
                    + "those transactions will be dropped.",
                    describeTransactionIds(contaminated.stream().map(CommittedTransaction::transactionId).toList()));
            return;
        }

        // Build inherited seeds for contaminated transactions that have pre-DDL events
        // (events at SCN < minDdlScn, which were parsed correctly and must be preserved).
        // The replay reads scn > (minDdlScn - 1), so the START row is NOT re-read; the
        // worker relies on these seeds to attach newly re-parsed post-DDL events.
        final List<InheritedTransaction> inheritedSeeds = new ArrayList<>();
        for (final CommittedTransaction tx : contaminated) {
            final List<LogMinerEvent> preDdlEvents = tx.events().stream()
                    .filter(e -> e.getScn().compareTo(finalMinDdlScn) < 0)
                    .toList();
            if (!preDdlEvents.isEmpty()) {
                inheritedSeeds.add(new InheritedTransaction(
                        tx.transactionId(),
                        tx.startScn(),
                        tx.commitTime(),
                        tx.userName(),
                        tx.clientId(),
                        tx.redoThreadId(),
                        finalMinDdlScn.subtract(Scn.ONE),
                        finalMinDdlScn.subtract(Scn.ONE),
                        List.copyOf(preDdlEvents)));
            }
        }

        final Scn replaySessionStart = replayLogs.get(0).getFirstScn();
        final Scn replayEnd = replayLogs.get(replayLogs.size() - 1).getNextScn();
        final Scn replayReadStart = finalMinDdlScn.subtract(Scn.ONE);

        pendingDdlContaminationUnits.add(new WorkUnit(
                WorkUnitType.WORKER,
                replayLogs,
                replaySessionStart,
                replayEnd,
                replayReadStart,
                replayEnd,
                List.copyOf(inheritedSeeds)));

        for (final CommittedTransaction tx : contaminated) {
            ddlContaminatedTransactionIds.add(tx.transactionId());
        }

        LOGGER.info("Scheduled DDL-contamination replay: logs={}, readRange=({},{}], inheritedSeeds={}, targetTransactions={}",
                describeLogs(replayLogs),
                replayReadStart,
                replayEnd,
                describeTransactionIds(inheritedSeeds.stream().map(InheritedTransaction::transactionId).toList()),
                describeTransactionIds(contaminated.stream().map(CommittedTransaction::transactionId).toList()));
    }
}
