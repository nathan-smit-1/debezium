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
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import io.debezium.connector.oracle.logminer.LogMinerSchemaChangeRecord;
import io.debezium.connector.oracle.logminer.TransactionCommitConsumer;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;

/**
 * Handles merging worker results, filtering overlapping transactions, applying schema changes,
 * and dispatching committed transactions in strict SCN order.
 *
 * <p>Operates on shared mutable state lists owned by the coordinator.
 *
 * @author Debezium Authors
 */
final class TransactionMerger {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionMerger.class);

    private final OracleConnectorConfig connectorConfig;
    private final OracleDatabaseSchema schema;
    private final AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics;

    // Shared mutable state -- references to coordinator-owned lists
    private final List<InheritedTransaction> pendingInheritedTransactions;
    private final List<OrphanCommit> pendingOrphanCommits;
    private final List<WorkUnit> pendingReplayUnits;
    private final List<CommittedTransaction> pendingDispatch;
    private final List<LogMinerSchemaChangeRecord> pendingSchemaChanges;
    private final List<WorkUnit> pendingDdlContaminationUnits;
    private final Set<String> ddlContaminatedTransactionIds;

    private final WorkUnitPlanner planner;

    TransactionMerger(OracleConnectorConfig connectorConfig,
                      OracleDatabaseSchema schema,
                      AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics,
                      List<InheritedTransaction> pendingInheritedTransactions,
                      List<OrphanCommit> pendingOrphanCommits,
                      List<WorkUnit> pendingReplayUnits,
                      List<CommittedTransaction> pendingDispatch,
                      List<LogMinerSchemaChangeRecord> pendingSchemaChanges,
                      List<WorkUnit> pendingDdlContaminationUnits,
                      Set<String> ddlContaminatedTransactionIds,
                      WorkUnitPlanner planner) {
        this.connectorConfig = connectorConfig;
        this.schema = schema;
        this.streamingMetrics = streamingMetrics;
        this.pendingInheritedTransactions = pendingInheritedTransactions;
        this.pendingOrphanCommits = pendingOrphanCommits;
        this.pendingReplayUnits = pendingReplayUnits;
        this.pendingDispatch = pendingDispatch;
        this.pendingSchemaChanges = pendingSchemaChanges;
        this.pendingDdlContaminationUnits = pendingDdlContaminationUnits;
        this.ddlContaminatedTransactionIds = ddlContaminatedTransactionIds;
        this.planner = planner;
    }

    Scn mergeAndDispatch(List<LogFile> availableLogs,
                         List<WorkerResult> results,
                         EventDispatcher<OraclePartition, TableId> dispatcher,
                         OraclePartition partition,
                         OracleOffsetContext offsetContext,
                         ZoneOffset databaseOffset,
                         boolean updatePendingReplayUnits,
                         List<LogMinerSchemaChangeRecord> appliedWaveSchemaChanges)
            throws InterruptedException {

        final List<CommittedTransaction> allResolved = new ArrayList<>(pendingDispatch);
        pendingDispatch.clear();

        final List<LogMinerSchemaChangeRecord> allSchemaChanges = new ArrayList<>(pendingSchemaChanges);
        pendingSchemaChanges.clear();
        Scn maxProcessedScn = Scn.NULL;

        for (WorkerResult result : results) {
            if (maxProcessedScn.isNull() || result.finalReadScn().compareTo(maxProcessedScn) > 0) {
                maxProcessedScn = result.finalReadScn();
            }
            updateCrossWaveState(result);
        }

        pruneResolvedCrossWaveState(results);

        final var continuationPlans = planner.planKnownCommitContinuations(pendingInheritedTransactions, pendingOrphanCommits);
        final Set<Integer> overlappingWorkerIds = planner.findOverlappingWorkerResults(results, continuationPlans).stream()
                .map(WorkerResult::workerId)
                .collect(Collectors.toSet());

        if (updatePendingReplayUnits) {
            pendingReplayUnits.clear();
            pendingReplayUnits.addAll(planner.planReplayWorkUnits(availableLogs, results, continuationPlans));
        }

        allResolved.removeIf(tx -> isRecoverableByKnownCommitContinuation(tx, continuationPlans));
        allSchemaChanges.removeIf(change -> isRecoverableByKnownCommitContinuation(change, continuationPlans));

        for (WorkerResult result : results) {
            allResolved.addAll(filterResolvedTransactions(result, continuationPlans, overlappingWorkerIds));
            allSchemaChanges.addAll(filterSchemaChanges(result, continuationPlans, overlappingWorkerIds));
        }

        deduplicateResolvedTransactions(allResolved);

        final List<LogMinerSchemaChangeRecord> contaminationSchemaChanges = new ArrayList<>(appliedWaveSchemaChanges);
        contaminationSchemaChanges.addAll(allSchemaChanges);

        allResolved.sort(Comparator.comparing(CommittedTransaction::commitScn)
                .thenComparing(CommittedTransaction::commitRsId, Comparator.nullsLast(Comparator.naturalOrder())));
        allSchemaChanges.sort(Comparator.comparing(LogMinerSchemaChangeRecord::scn));

        if (!contaminationSchemaChanges.isEmpty()) {
            final List<CommittedTransaction> contaminated = detectDdlContaminatedTransactions(allResolved, contaminationSchemaChanges);
            if (!contaminated.isEmpty()) {
                LOGGER.warn("Detected {} DDL-contaminated transaction(s); quarantining for replay: {}",
                        contaminated.size(),
                        describeTransactionIds(contaminated.stream().map(CommittedTransaction::transactionId).toList()));
                allResolved.removeAll(contaminated);
                scheduleContaminatedTransactionReplay(availableLogs, contaminated, contaminationSchemaChanges);
            }
        }

        final Scn safeHorizon = computeSafeDispatchHorizon();
        if (safeHorizon != null) {
            LOGGER.info("Safe dispatch horizon active: {}.", safeHorizon);
        }

        final List<LogMinerSchemaChangeRecord> newlyAppliedSchemaChanges = new ArrayList<>();

        dispatchTransactionsWithSchemaChanges(allResolved, allSchemaChanges, dispatcher, partition,
                offsetContext, databaseOffset, safeHorizon, newlyAppliedSchemaChanges);

        appliedWaveSchemaChanges.addAll(newlyAppliedSchemaChanges);

        if (!maxProcessedScn.isNull()) {
            offsetContext.setScn(maxProcessedScn);
            dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
        }

        LOGGER.info("Concurrent wave merge complete: dispatched={}, heldBack={}, schemaChanges={}, pendingInherited={}, pendingOrphans={}, nextScn={}",
                allResolved.size() - pendingDispatch.size(), pendingDispatch.size(), allSchemaChanges.size(),
                pendingInheritedTransactions.size(), pendingOrphanCommits.size(), maxProcessedScn);

        return maxProcessedScn;
    }

    void dispatchPendingBeforeSerial(EventDispatcher<OraclePartition, TableId> dispatcher,
                                     OraclePartition partition,
                                     OracleOffsetContext offsetContext,
                                     ZoneOffset databaseOffset)
            throws InterruptedException {

        if (pendingDispatch.isEmpty() && pendingSchemaChanges.isEmpty()) {
            return;
        }

        final List<CommittedTransaction> heldBackTransactions = new ArrayList<>(pendingDispatch);
        final List<LogMinerSchemaChangeRecord> heldBackSchemaChanges = new ArrayList<>(pendingSchemaChanges);
        pendingDispatch.clear();
        pendingSchemaChanges.clear();

        heldBackTransactions.sort(Comparator.comparing(CommittedTransaction::commitScn)
                .thenComparing(CommittedTransaction::commitRsId, Comparator.nullsLast(Comparator.naturalOrder())));
        heldBackSchemaChanges.sort(Comparator.comparing(LogMinerSchemaChangeRecord::scn));

        dispatchTransactionsWithSchemaChanges(heldBackTransactions, heldBackSchemaChanges, dispatcher,
                partition, offsetContext, databaseOffset, null, null);
    }

    void dispatchCommittedTransaction(CommittedTransaction tx,
                                      EventDispatcher<OraclePartition, TableId> dispatcher,
                                      OraclePartition partition,
                                      OracleOffsetContext offsetContext,
                                      ZoneOffset databaseOffset)
            throws InterruptedException {

        if (tx.events().isEmpty()) {
            offsetContext.setScn(tx.commitScn());
            dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            return;
        }

        final boolean skipEvents = LogMinerEventDispatchHelper.isTransactionSkippedAtCommit(
                connectorConfig, tx.userName(), tx.clientId());
        if (skipEvents) {
            LOGGER.debug("Skipping transaction {} because it matches exclude/include filters.", tx.transactionId());
            offsetContext.setScn(tx.commitScn());
            dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            return;
        }

        final String transactionId = tx.transactionId();
        final Scn commitScn = tx.commitScn();

        final TransactionCommitConsumer.Handler<LogMinerEvent> delegate = (event, eventIndex, eventTrxId, eventTrxSeq, eventsProcessed) -> {
            LogMinerEventDispatchHelper.dispatchDataChangeEvent(connectorConfig, dispatcher, partition, offsetContext,
                    schema, databaseOffset, transactionId, tx.userName(), tx.redoThreadId(), commitScn,
                    tx.commitTime(), (DmlEvent) event, eventIndex);
        };

        try (TransactionCommitConsumer commitConsumer = new TransactionCommitConsumer(delegate, connectorConfig, schema)) {
            for (LogMinerEvent event : tx.events()) {
                commitConsumer.accept(event, false, null, 0L);
            }
        }

        LogMinerEventDispatchHelper.finishCommittedTransaction(dispatcher, partition, offsetContext,
                tx.redoThreadId(), transactionId, commitScn, tx.commitRsId(),
                tx.commitTime() != null ? tx.commitTime() : Instant.now(), true);

        offsetContext.setScn(commitScn);
    }

    void applySchemaChange(LogMinerSchemaChangeRecord change,
                           EventDispatcher<OraclePartition, TableId> dispatcher,
                           OraclePartition partition,
                           OracleOffsetContext offsetContext)
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

            if (LogMinerEventDispatchHelper.isSchemaChangeSkippedByUser(connectorConfig, change.redoSql())) {
                return;
            }

            if (LogMinerEventDispatchHelper.isSchemaChangeSkippedByTableFilter(connectorConfig, schema, tableId)) {
                return;
            }

            offsetContext.setScn(change.scn());
            final TruncateReceiver noOpTruncate = () -> {
            };
            LogMinerEventDispatchHelper.dispatchSchemaChangeEvent(connectorConfig, dispatcher, partition,
                    offsetContext, schema, streamingMetrics, noOpTruncate, change);
        }
        catch (Exception e) {
            LOGGER.warn("Failed to apply schema change at SCN {}: {}", change.scn(), e.getMessage());
        }
    }

    void updateCrossWaveState(WorkerResult result) {
        final Map<String, InheritedTransaction> existingById = new HashMap<>();
        for (InheritedTransaction t : pendingInheritedTransactions) {
            existingById.put(t.transactionId(), t);
        }
        for (InheritedTransaction newTx : result.unresolvedTransactions()) {
            existingById.put(newTx.transactionId(), newTx);
        }
        pendingInheritedTransactions.clear();
        pendingInheritedTransactions.addAll(existingById.values());

        final Set<OrphanCommitKey> knownOrphans = pendingOrphanCommits.stream()
                .map(orphan -> new OrphanCommitKey(orphan.transactionId(), orphan.commitScn()))
                .collect(Collectors.toCollection(HashSet::new));
        for (OrphanCommit orphan : result.orphanCommits()) {
            if (knownOrphans.add(new OrphanCommitKey(orphan.transactionId(), orphan.commitScn()))) {
                pendingOrphanCommits.add(orphan);
            }
        }

        LOGGER.info("Updated cross-wave state from worker {}: unresolvedNow={}, orphansNow={}, unresolvedTransactions={}, orphanTransactions={}",
                result.workerId(), pendingInheritedTransactions.size(), pendingOrphanCommits.size(),
                describeTransactionIds(pendingInheritedTransactions.stream().map(InheritedTransaction::transactionId).toList()),
                describeTransactionIds(pendingOrphanCommits.stream().map(OrphanCommit::transactionId).toList()));
    }

    // ------------------------------------------------------------------ private helpers

    private Scn computeSafeDispatchHorizon() {
        if (pendingInheritedTransactions.isEmpty()) {
            return null;
        }

        final Map<String, Scn> orphanScnByTxId = new HashMap<>();
        for (OrphanCommit orphan : pendingOrphanCommits) {
            orphanScnByTxId.put(orphan.transactionId(), orphan.commitScn());
        }

        Scn horizon = null;
        for (InheritedTransaction unresolved : pendingInheritedTransactions) {
            final Scn matchedCommitScn = orphanScnByTxId.get(unresolved.transactionId());
            final Scn candidate = matchedCommitScn != null ? matchedCommitScn : unresolved.lastReadScn();
            if (horizon == null || candidate.compareTo(horizon) < 0) {
                horizon = candidate;
            }
        }
        return horizon;
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

    private List<CommittedTransaction> filterResolvedTransactions(WorkerResult result,
                                                                   List<WorkUnitPlanner.KnownCommitContinuationPlan> continuationPlans,
                                                                   Set<Integer> overlappingWorkerIds) {
        if (result.unitType() != WorkUnitType.WORKER || !overlappingWorkerIds.contains(result.workerId())) {
            return result.resolvedTransactions();
        }

        final var overlappingPlans = continuationPlans.stream()
                .filter(plan -> planner.overlapsUnsafeInterval(result, plan)).toList();

        final Scn cutoff = overlappingPlans.stream()
                .map(WorkUnitPlanner.KnownCommitContinuationPlan::unsafeIntervalStartScn)
                .min(Comparator.naturalOrder()).orElse(null);
        final Scn replayStart = overlappingPlans.stream()
                .map(WorkUnitPlanner.KnownCommitContinuationPlan::unsafeIntervalEndScn)
                .max(Comparator.naturalOrder()).orElse(null);

        if (cutoff == null || replayStart == null) {
            return result.resolvedTransactions();
        }

        return result.resolvedTransactions().stream()
                .filter(tx -> tx.commitScn().compareTo(cutoff) <= 0 || tx.commitScn().compareTo(replayStart) > 0)
                .toList();
    }

    private List<LogMinerSchemaChangeRecord> filterSchemaChanges(WorkerResult result,
                                                                       List<WorkUnitPlanner.KnownCommitContinuationPlan> continuationPlans,
                                                                       Set<Integer> overlappingWorkerIds) {
        if (result.unitType() != WorkUnitType.WORKER || !overlappingWorkerIds.contains(result.workerId())) {
            return result.schemaChanges();
        }

        final var overlappingPlans = continuationPlans.stream()
                .filter(plan -> planner.overlapsUnsafeInterval(result, plan)).toList();

        final Scn cutoff = overlappingPlans.stream()
                .map(WorkUnitPlanner.KnownCommitContinuationPlan::unsafeIntervalStartScn)
                .min(Comparator.naturalOrder()).orElse(null);
        final Scn replayStart = overlappingPlans.stream()
                .map(WorkUnitPlanner.KnownCommitContinuationPlan::unsafeIntervalEndScn)
                .max(Comparator.naturalOrder()).orElse(null);

        if (cutoff == null || replayStart == null) {
            return result.schemaChanges();
        }

        return result.schemaChanges().stream()
                .filter(change -> change.scn().compareTo(cutoff) <= 0 || change.scn().compareTo(replayStart) > 0)
                .toList();
    }

    private boolean isRecoverableByKnownCommitContinuation(CommittedTransaction transaction,
                                                            List<WorkUnitPlanner.KnownCommitContinuationPlan> plans) {
        return plans.stream().anyMatch(plan -> transaction.commitScn().compareTo(plan.unsafeIntervalStartScn()) > 0
                && transaction.commitScn().compareTo(plan.unsafeIntervalEndScn()) <= 0);
    }

    private boolean isRecoverableByKnownCommitContinuation(LogMinerSchemaChangeRecord change,
                                                            List<WorkUnitPlanner.KnownCommitContinuationPlan> plans) {
        return plans.stream().anyMatch(plan -> change.scn().compareTo(plan.unsafeIntervalStartScn()) > 0
                && change.scn().compareTo(plan.unsafeIntervalEndScn()) <= 0);
    }

    private void deduplicateResolvedTransactions(List<CommittedTransaction> resolvedTransactions) {
        if (resolvedTransactions.size() < 2) {
            return;
        }
        final Map<String, CommittedTransaction> latestById = new java.util.LinkedHashMap<>();
        for (CommittedTransaction tx : resolvedTransactions) {
            latestById.put(tx.transactionId(), tx);
        }
        if (latestById.size() == resolvedTransactions.size()) {
            return;
        }
        resolvedTransactions.clear();
        resolvedTransactions.addAll(latestById.values());
    }

    private void dispatchTransactionsWithSchemaChanges(List<CommittedTransaction> transactions,
                                                        List<LogMinerSchemaChangeRecord> schemaChanges,
                                                        EventDispatcher<OraclePartition, TableId> dispatcher,
                                                        OraclePartition partition,
                                                        OracleOffsetContext offsetContext,
                                                        ZoneOffset databaseOffset,
                                                        Scn safeHorizon,
                                                        List<LogMinerSchemaChangeRecord> appliedSchemaChanges)
            throws InterruptedException {

        final Iterator<LogMinerSchemaChangeRecord> schemaIter = schemaChanges.iterator();
        LogMinerSchemaChangeRecord pendingSchemaChange = schemaIter.hasNext() ? schemaIter.next() : null;

        for (CommittedTransaction transaction : transactions) {
            if (safeHorizon != null && transaction.commitScn().compareTo(safeHorizon) >= 0) {
                pendingDispatch.add(transaction);
                continue;
            }

            while (pendingSchemaChange != null && pendingSchemaChange.scn().compareTo(transaction.commitScn()) <= 0) {
                applySchemaChange(pendingSchemaChange, dispatcher, partition, offsetContext);
                if (appliedSchemaChanges != null) {
                    appliedSchemaChanges.add(pendingSchemaChange);
                }
                pendingSchemaChange = schemaIter.hasNext() ? schemaIter.next() : null;
            }

            dispatchCommittedTransaction(transaction, dispatcher, partition, offsetContext, databaseOffset);
        }

        while (pendingSchemaChange != null) {
            if (safeHorizon != null && pendingSchemaChange.scn().compareTo(safeHorizon) >= 0) {
                pendingSchemaChanges.add(pendingSchemaChange);
                schemaIter.forEachRemaining(pendingSchemaChanges::add);
                break;
            }
            applySchemaChange(pendingSchemaChange, dispatcher, partition, offsetContext);
            if (appliedSchemaChanges != null) {
                appliedSchemaChanges.add(pendingSchemaChange);
            }
            pendingSchemaChange = schemaIter.hasNext() ? schemaIter.next() : null;
        }
    }

    private List<CommittedTransaction> detectDdlContaminatedTransactions(List<CommittedTransaction> resolved,
                                                                         List<LogMinerSchemaChangeRecord> schemaChanges) {
        if (schemaChanges.isEmpty() || resolved.isEmpty()) {
            return List.of();
        }
        final Map<TableId, Scn> earliestDdlScnByTable = new HashMap<>();
        for (final LogMinerSchemaChangeRecord ddl : schemaChanges) {
            if (ddl.tableId() != null) {
                earliestDdlScnByTable.merge(ddl.tableId(), ddl.scn(),
                        (existing, candidate) -> existing.compareTo(candidate) <= 0 ? existing : candidate);
            }
        }
        return resolved.stream()
                .filter(tx -> tx.events().stream().anyMatch(event -> {
                    final Scn ddlScn = earliestDdlScnByTable.get(event.getTableId());
                    return ddlScn != null && event.getScn().compareTo(ddlScn) > 0;
                }))
                .toList();
    }

    private void scheduleContaminatedTransactionReplay(List<LogFile> availableLogs,
                                                        List<CommittedTransaction> contaminated,
                                                        List<LogMinerSchemaChangeRecord> schemaChanges) {
        final Map<TableId, Scn> earliestDdlScnByTable = new HashMap<>();
        for (final LogMinerSchemaChangeRecord ddl : schemaChanges) {
            if (ddl.tableId() != null) {
                earliestDdlScnByTable.merge(ddl.tableId(), ddl.scn(),
                        (existing, candidate) -> existing.compareTo(candidate) <= 0 ? existing : candidate);
            }
        }

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

        final Scn maxCommitScn = contaminated.stream().map(CommittedTransaction::commitScn)
                .max(Comparator.naturalOrder()).orElse(null);
        if (maxCommitScn == null) {
            return;
        }

        final Scn finalMinDdlScn = minDdlScn;
        final List<LogFile> replayLogs = availableLogs.stream()
                .filter(log -> !log.getNextScn().isNull()
                        && log.getNextScn().compareTo(finalMinDdlScn) > 0
                        && log.getFirstScn().compareTo(maxCommitScn) <= 0)
                .sorted(Comparator.comparing(LogFile::getFirstScn)).toList();

        if (replayLogs.isEmpty()) {
            LOGGER.warn("No archive logs available to replay DDL-contaminated transactions {}",
                    describeTransactionIds(contaminated.stream().map(CommittedTransaction::transactionId).toList()));
            return;
        }

        final List<InheritedTransaction> inheritedSeeds = new ArrayList<>();
        for (final CommittedTransaction tx : contaminated) {
            final List<LogMinerEvent> preDdlEvents = tx.events().stream()
                    .filter(e -> e.getScn().compareTo(finalMinDdlScn) < 0).toList();
            if (!preDdlEvents.isEmpty()) {
                inheritedSeeds.add(new InheritedTransaction(tx.transactionId(), tx.startScn(), tx.commitTime(),
                        tx.userName(), tx.clientId(), tx.redoThreadId(),
                        finalMinDdlScn.subtract(Scn.ONE), finalMinDdlScn.subtract(Scn.ONE), List.copyOf(preDdlEvents)));
            }
        }

        final Scn replaySessionStart = replayLogs.get(0).getFirstScn();
        final Scn replayEnd = replayLogs.get(replayLogs.size() - 1).getNextScn();
        final Scn replayReadStart = finalMinDdlScn.subtract(Scn.ONE);

        pendingDdlContaminationUnits.add(new WorkUnit(WorkUnitType.WORKER, replayLogs,
                replaySessionStart, replayEnd, replayReadStart, replayEnd, List.copyOf(inheritedSeeds)));

        for (final CommittedTransaction tx : contaminated) {
            ddlContaminatedTransactionIds.add(tx.transactionId());
        }

        LOGGER.info("Scheduled DDL-contamination replay: logs={}, readRange=({},{}], inheritedSeeds={}, targetTransactions={}",
                describeLogs(replayLogs), replayReadStart, replayEnd,
                describeTransactionIds(inheritedSeeds.stream().map(InheritedTransaction::transactionId).toList()),
                describeTransactionIds(contaminated.stream().map(CommittedTransaction::transactionId).toList()));
    }

    private record OrphanCommitKey(String transactionId, Scn commitScn) {
    }
}
