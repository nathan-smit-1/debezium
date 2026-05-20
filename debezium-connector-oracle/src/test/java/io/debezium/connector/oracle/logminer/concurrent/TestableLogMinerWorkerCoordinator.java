/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.concurrent;

import static io.debezium.connector.oracle.logminer.concurrent.LogMinerWorkerDescriptions.describeLogs;
import static io.debezium.connector.oracle.logminer.concurrent.LogMinerWorkerDescriptions.describeTransactionIds;

import java.lang.reflect.Field;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.AbstractOracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogFile;
import io.debezium.connector.oracle.logminer.LogMinerSchemaChangeRecord;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;

/**
 * A test-specific subclass of {@link LogMinerWorkerCoordinator} that replaces
 * real {@link LogMinerWorker} instances with {@link SimulatedLogMinerWorker}
 * instances backed by a {@link LogMinerEventSimulator}.
 *
 * <p>This allows unit tests to verify coordinator behavior (work unit planning,
 * bridge creation, merge-sort dispatch, replay scheduling) without requiring a
 * live Oracle database.
 *
 * @author Debezium Authors
 */
public class TestableLogMinerWorkerCoordinator extends LogMinerWorkerCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestableLogMinerWorkerCoordinator.class);

    private final LogMinerEventSimulator simulator;
    private final OracleConnectorConfig connectorConfig;
    private final OracleDatabaseSchema schema;

    public TestableLogMinerWorkerCoordinator(OracleConnectorConfig connectorConfig,
                                             OracleDatabaseSchema schema,
                                             JdbcConfiguration jdbcConfig,
                                             AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics,
                                             LogMinerEventSimulator simulator) {
        super(connectorConfig, schema, jdbcConfig, streamingMetrics);
        this.simulator = simulator;
        this.connectorConfig = connectorConfig;
        this.schema = schema;
    }

    @Override
    protected List<WorkerResult> executeWorkers(List<WorkUnit> units) throws InterruptedException {
        final java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(units.size(), r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        try {
            final List<Future<WorkerResult>> futures = new ArrayList<>(units.size());
            for (int i = 0; i < units.size(); i++) {
                futures.add(executor.submit(
                        new SimulatedLogMinerWorker(i, units.get(i), simulator, connectorConfig, schema)));
            }
            final List<WorkerResult> results = new ArrayList<>(futures.size());
            for (Future<WorkerResult> future : futures) {
                try {
                    results.add(future.get());
                }
                catch (ExecutionException e) {
                    final Throwable cause = e.getCause();
                    if (cause instanceof RuntimeException rte) {
                        throw rte;
                    }
                    throw new RuntimeException("Simulated LogMiner worker failed", cause);
                }
            }
            return results;
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Override
    protected void executeWorkersAsCompleted(List<WorkUnit> units, WorkerResultConsumer consumer) throws InterruptedException {
        final java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(units.size(), r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        try {
            final java.util.concurrent.ExecutorCompletionService<WorkerResult> completionService = new java.util.concurrent.ExecutorCompletionService<>(executor);
            for (int i = 0; i < units.size(); i++) {
                completionService.submit(new SimulatedLogMinerWorker(i, units.get(i), simulator, connectorConfig, schema));
            }
            for (int i = 0; i < units.size(); i++) {
                try {
                    consumer.accept(completionService.take().get());
                }
                catch (ExecutionException e) {
                    final Throwable cause = e.getCause();
                    if (cause instanceof RuntimeException rte) {
                        throw rte;
                    }
                    throw new RuntimeException("Simulated LogMiner worker failed", cause);
                }
            }
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Override
    public Scn executeSerial(Scn readStartScn,
                             Scn readEndScn,
                             List<LogFile> allLogs,
                             EventDispatcher<OraclePartition, TableId> dispatcher,
                             OraclePartition partition,
                             OracleOffsetContext offsetContext,
                             ZoneOffset databaseOffset) {
        try {
            final WorkUnitPlanner planner = (WorkUnitPlanner) getParentField("planner");
            final TransactionMerger merger = (TransactionMerger) getParentField("merger");
            final List<InheritedTransaction> pendingInherited = (List<InheritedTransaction>) getParentField("pendingInheritedTransactions");
            final List<OrphanCommit> pendingOrphans = (List<OrphanCommit>) getParentField("pendingOrphanCommits");

            final WorkUnit unit = planner.buildSerialWorkUnit(allLogs, readStartScn, readEndScn, pendingInherited);

            LOGGER.info("Starting serial wave (simulated): logs={}, session=[{},{}], read=[{},{}], inheritedTransactions={}",
                    describeLogs(unit.logFiles()), unit.sessionStartScn(), unit.sessionEndScn(),
                    unit.readStartScn(), unit.readEndScn(),
                    describeTransactionIds(pendingInherited.stream()
                            .map(InheritedTransaction::transactionId)
                            .toList()));

            merger.dispatchPendingBeforeSerial(dispatcher, partition, offsetContext, databaseOffset);

            final WorkerResult result = new SimulatedLogMinerWorker(0, unit, simulator, connectorConfig, schema).call();

            final Set<String> resolvedTransactionIds = new HashSet<>();
            for (CommittedTransaction tx : result.resolvedTransactions()) {
                resolvedTransactionIds.add(tx.transactionId());
                merger.dispatchCommittedTransaction(tx, dispatcher, partition, offsetContext, databaseOffset);
            }
            for (LogMinerSchemaChangeRecord change : result.schemaChanges()) {
                merger.applySchemaChange(change, dispatcher, partition, offsetContext);
            }

            pendingInherited.removeIf(tx -> resolvedTransactionIds.contains(tx.transactionId()));
            pendingOrphans.removeIf(orphan -> resolvedTransactionIds.contains(orphan.transactionId()));
            merger.updateCrossWaveState(result);

            if (!result.finalReadScn().isNull()) {
                offsetContext.setScn(result.finalReadScn());
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }

            LOGGER.info("Serial wave (simulated) complete: dispatched={}, unresolvedNow={}, orphansNow={}, nextScn={}",
                    resolvedTransactionIds.size(), pendingInherited.size(), pendingOrphans.size(), result.finalReadScn());

            return result.finalReadScn();
        }
        catch (Exception e) {
            throw new RuntimeException("Simulated serial wave failed", e);
        }
    }

    private Object getParentField(String name) throws NoSuchFieldException, IllegalAccessException {
        Field field = LogMinerWorkerCoordinator.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(this);
    }
}
