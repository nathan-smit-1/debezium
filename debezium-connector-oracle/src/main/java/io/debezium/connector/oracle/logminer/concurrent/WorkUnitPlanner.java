/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.concurrent;

import static io.debezium.connector.oracle.logminer.concurrent.LogMinerWorkerDescriptions.describeLogs;
import static io.debezium.connector.oracle.logminer.concurrent.LogMinerWorkerDescriptions.describeTransactionIds;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogFile;

/**
 * Stateless planner that partitions archive log files into {@link WorkUnit}s for concurrent
 * LogMiner processing.
 *
 * <p>All methods are pure functions: they take the current pending state as parameters and
 * return work units without side effects, except {@link #buildWorkUnits} which removes matched
 * orphan/inherited pairs consumed by BRIDGE units.
 *
 * @author Debezium Authors
 */
final class WorkUnitPlanner {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkUnitPlanner.class);

    private final int concurrentReaders;

    WorkUnitPlanner(int concurrentReaders) {
        this.concurrentReaders = concurrentReaders;
    }

    // ------------------------------------------------------------------ top-level planning

    /**
     * Partitions the sorted archive log list into work units.
     *
     * <p>BRIDGE units are created first for matched orphan+inherited pairs, then WORKER units
     * are created for the remaining logs. Matched pairs are removed from the pending lists.
     */
    List<WorkUnit> buildWorkUnits(List<LogFile> sortedLogs,
                                  Scn readStartScn,
                                  List<InheritedTransaction> pendingInheritedTransactions,
                                  List<OrphanCommit> pendingOrphanCommits) {
        final List<WorkUnit> units = new ArrayList<>();

        final List<WorkUnit> bridgeUnits = buildBridgeUnits(sortedLogs, pendingInheritedTransactions, pendingOrphanCommits);
        units.addAll(bridgeUnits);

        final List<LogFile> workerLogs = getWorkerLogs(sortedLogs, bridgeUnits);
        if (!workerLogs.isEmpty()) {
            units.addAll(buildWorkerUnits(workerLogs, readStartScn));
        }

        return units;
    }

    WorkUnit buildSerialWorkUnit(List<LogFile> allLogs,
                                 Scn readStartScn,
                                 Scn readEndScn,
                                 List<InheritedTransaction> pendingInheritedTransactions) {
        final List<LogFile> sortedLogs = allLogs.stream()
                .sorted(Comparator.comparing(LogFile::getFirstScn))
                .toList();

        final Scn sessionStartScn;
        if (!readStartScn.isNull() && pendingInheritedTransactions.isEmpty()) {
            sessionStartScn = readStartScn;
        }
        else {
            sessionStartScn = pendingInheritedTransactions.stream()
                    .map(InheritedTransaction::startScn)
                    .min(Comparator.naturalOrder())
                    .map(startScn -> startScn.subtract(Scn.ONE))
                    .orElseGet(() -> sortedLogs.stream()
                            .map(LogFile::getFirstScn)
                            .min(Comparator.naturalOrder())
                            .orElse(Scn.NULL));
        }

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

    void logPlannedWorkUnits(List<WorkUnit> units) {
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

    // ------------------------------------------------------------------ known-commit continuation planning

    List<KnownCommitContinuationPlan> planKnownCommitContinuations(List<LogFile> sortedLogs,
                                                                    List<InheritedTransaction> pendingInheritedTransactions,
                                                                    List<OrphanCommit> pendingOrphanCommits) {
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

    List<KnownCommitContinuationPlan> planKnownCommitContinuations(List<InheritedTransaction> pendingInheritedTransactions,
                                                                    List<OrphanCommit> pendingOrphanCommits) {
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

    // ------------------------------------------------------------------ overlap and replay planning

    List<WorkerResult> findOverlappingWorkerResults(List<WorkerResult> results,
                                                     List<KnownCommitContinuationPlan> continuationPlans) {
        if (results.isEmpty() || continuationPlans.isEmpty()) {
            return List.of();
        }

        return results.stream()
                .filter(result -> result.unitType() == WorkUnitType.WORKER)
                .filter(result -> continuationPlans.stream().anyMatch(plan -> overlapsUnsafeInterval(result, plan)))
                .toList();
    }

    List<WorkUnit> planReplayWorkUnits(List<LogFile> sortedLogs,
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

    // ------------------------------------------------------------------ bridge construction

    /**
     * Builds BRIDGE units for pending orphan commits that have matching pending inherited
     * transactions. Matched pairs are removed from the pending lists.
     */
    List<WorkUnit> buildBridgeUnits(List<LogFile> sortedLogs,
                                    List<InheritedTransaction> pendingInheritedTransactions,
                                    List<OrphanCommit> pendingOrphanCommits) {
        final List<WorkUnit> bridges = new ArrayList<>();

        final Map<String, BridgeBatch> batches = new LinkedHashMap<>();
        final List<OrphanCommit> matchedOrphans = new ArrayList<>();
        final List<InheritedTransaction> matchedInherited = new ArrayList<>();

        for (KnownCommitContinuationPlan plan : planKnownCommitContinuations(sortedLogs, pendingInheritedTransactions, pendingOrphanCommits)) {
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

    // ------------------------------------------------------------------ worker unit construction

    /**
     * Distributes the worker logs contiguously across {@code concurrentReaders} work units.
     */
    List<WorkUnit> buildWorkerUnits(List<LogFile> workerLogs, Scn readStartScn) {
        final List<WorkUnit> units = new ArrayList<>();
        final int readers = Math.min(concurrentReaders, workerLogs.size());
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

    // ------------------------------------------------------------------ log filtering helpers

    /**
     * Returns logs whose read slices do not overlap any BRIDGE unsafe interval.
     */
    List<LogFile> getWorkerLogs(List<LogFile> sortedLogs, List<WorkUnit> bridgeUnits) {
        return sortedLogs.stream()
                .filter(log -> bridgeUnits.stream().noneMatch(unit -> overlapsBridgeUnsafeInterval(log, unit)))
                .toList();
    }

    boolean overlapsUnsafeInterval(WorkerResult result, KnownCommitContinuationPlan plan) {
        return result.finalReadScn().compareTo(plan.unsafeIntervalStartScn()) > 0
                && result.readStartScn().compareTo(plan.unsafeIntervalEndScn()) < 0;
    }

    // ------------------------------------------------------------------ private helpers

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

    // ------------------------------------------------------------------ inner types

    static final class BridgeBatch {
        final List<LogFile> logFiles;
        final Scn unsafeIntervalStartScn;
        final List<InheritedTransaction> inheritedTransactions = new ArrayList<>();
        Scn earliestStartScn;
        Scn latestCommitScn;

        BridgeBatch(List<LogFile> logFiles, Scn unsafeIntervalStartScn) {
            this.logFiles = logFiles;
            this.unsafeIntervalStartScn = unsafeIntervalStartScn;
        }

        void add(KnownCommitContinuationPlan plan) {
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
}
