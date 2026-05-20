/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.AbstractOracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogFile;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

/**
 * Unit tests for {@link LogMinerWorkerCoordinator} using additional simulation templates.
 *
 * <p>All tests use the same production-like setup: 2 concurrent readers and a
 * {@code log.mining.log.count.min=2} cap, so that the {@code CappedLogFileSessionSelector}
 * behaviour is faithfully emulated.
 *
 * @author Debezium Authors
 */
public class LogMinerWorkerCoordinatorAdditionalScenariosTest {

    private static final ZoneOffset DATABASE_OFFSET = ZoneOffset.UTC;
    private static final long SIMULATED_REDO_LOG_SIZE = 1024L;

    private OracleConnectorConfig connectorConfig;
    private OracleDatabaseSchema schema;
    private AbstractOracleStreamingChangeEventSourceMetrics metrics;
    private EventDispatcher<OraclePartition, TableId> dispatcher;
    private OraclePartition partition;
    private OracleOffsetContext offsetContext;

    @BeforeEach
    void setUp() {
        Configuration configuration = Configuration.create()
                .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "logminer")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "server1")
                .with(OracleConnectorConfig.HOSTNAME, "localhost")
                .with(OracleConnectorConfig.DATABASE_NAME, "ORCL")
                .with(OracleConnectorConfig.USER, "debezium")
                .with(OracleConnectorConfig.LOG_MINING_CONCURRENT_READERS, 2)
                .with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "localhost:9092")
                .with(KafkaSchemaHistory.TOPIC, "history")
                .build();

        connectorConfig = new OracleConnectorConfig(configuration);
        schema = mock(OracleDatabaseSchema.class);
        metrics = mock(AbstractOracleStreamingChangeEventSourceMetrics.class);
        dispatcher = mock(EventDispatcher.class);
        partition = mock(OraclePartition.class);
        offsetContext = mock(OracleOffsetContext.class);
    }

    // ------------------------------------------------------------------
    // Scenario 1: rollback-and-ddl
    // ------------------------------------------------------------------

    @Test
    void shouldHandleRollbackAndDdl() throws Exception {
        LogMinerEventSimulator simulator = new LogMinerEventSimulator();
        simulator.loadCsv(getClass().getResourceAsStream("/concurrent/rollback-and-ddl.csv"));
        List<LogFile> allLogs = simulator.getLogFiles();
        assertThat(allLogs).hasSize(2);

        try (TestableLogMinerWorkerCoordinator coordinator = new TestableLogMinerWorkerCoordinator(
                connectorConfig, schema, JdbcConfiguration.adapt(Configuration.create().build()), metrics, simulator)) {

            // Cap to 2 logs for the concurrent wave
            List<LogFile> sessionLogs = capLogs(allLogs, connectorConfig.getLogMiningMinimumLogCount());

            Scn processedScn = coordinator.executeWave(
                    Scn.valueOf(0), sessionLogs, dispatcher, partition, offsetContext, DATABASE_OFFSET);

            // TX_RB1 started in a_log1 but rolled back in a_log2.
            // The rollback is seen by worker 1 but TX_RB1 is not in worker 1's cache,
            // so it becomes an inherited transaction.
            assertThat(coordinator.getPendingInheritedTransactions())
                    .anyMatch(tx -> tx.transactionId().equals("TX_RB1"));

            // TX_OK is fully resolved in a_log2 but held back by the safe horizon.
            assertThat(coordinator.hasPendingUnknownCommitTransactions()).isTrue();

            // One serial fallback attempt. NOTE: the rollback for TX_RB1 is at SCN 12,
            // which is before the serial read start (processedScn ≈ 18). The serial worker
            // therefore cannot discover the rollback, so TX_RB1 remains unresolved.
            // This is a known limitation of the current concurrent approach for
            // cross-log rollbacks.
            processedScn = coordinator.executeSerial(
                    processedScn, Scn.NULL, sessionLogs,
                    dispatcher, partition, offsetContext, DATABASE_OFFSET);

            // TX_OK should be dispatched; TX_RB1 should NOT produce a committed event.
            verify(dispatcher, times(1)).dispatchTransactionCommittedEvent(any(), any(), any());
            verify(dispatcher, org.mockito.Mockito.atLeast(1)).dispatchHeartbeatEvent(any(), any());

            // TX_RB1 remains unresolved because its rollback was before the serial read start.
            assertThat(coordinator.getPendingInheritedTransactions())
                    .anyMatch(tx -> tx.transactionId().equals("TX_RB1"));
        }
        finally {
            simulator.close();
        }
    }

    // ------------------------------------------------------------------
    // Scenario 2: three-archive-one-redo
    // ------------------------------------------------------------------

    @Test
    void shouldProcessArchiveLogsConcurrentlyThenRedoSerially() throws Exception {
        LogMinerEventSimulator simulator = new LogMinerEventSimulator();
        simulator.loadCsv(getClass().getResourceAsStream("/concurrent/three-archive-one-redo.csv"));
        List<LogFile> allLogs = simulator.getLogFiles();
        assertThat(allLogs).hasSize(4);

        try (TestableLogMinerWorkerCoordinator coordinator = new TestableLogMinerWorkerCoordinator(
                connectorConfig, schema, JdbcConfiguration.adapt(Configuration.create().build()), metrics, simulator)) {

            // Concurrent reading IS applicable because there are 3 archive logs.
            // The redo log does not prevent concurrent archive processing.
            assertThat(coordinator.isConcurrentReadingApplicable(allLogs)).isTrue();

            // Phase 1: concurrent wave over archive logs (a_log1, a_log2, a_log3).
            // With 2 readers and a 2-log cap, worker0 gets a_log1+a_log2 and
            // worker1 gets a_log3.
            // - TX1 is fully in a_log1 -> resolved by worker0.
            // - TX2 spans a_log2->a_log3 -> inherited by worker0, orphan commit
            // seen by worker1 -> resolved by bridge.
            // - TX3 STARTs in a_log3 but COMMIT is in redo01 -> inherited.
            List<LogFile> archiveLogs = allLogs.stream().filter(LogFile::isArchive).toList();
            Scn processedScn = coordinator.executeWave(
                    Scn.valueOf(0), archiveLogs, dispatcher, partition, offsetContext, DATABASE_OFFSET);

            assertThat(coordinator.getPendingInheritedTransactions())
                    .anyMatch(tx -> tx.transactionId().equals("TX3"));

            // Bridge follow-up resolves TX2.
            if (coordinator.hasPendingBridgeTransactions()) {
                processedScn = coordinator.executePendingBridges(
                        archiveLogs, dispatcher, partition, offsetContext, DATABASE_OFFSET);
            }

            assertThat(coordinator.getPendingInheritedTransactions())
                    .hasSize(1)
                    .anyMatch(tx -> tx.transactionId().equals("TX3"));

            // Phase 2: serial over redo log to resolve TX3.
            List<LogFile> redoLogs = allLogs.stream().filter(LogFile::isRedo).toList();
            processedScn = coordinator.executeSerial(
                    processedScn, Scn.NULL, redoLogs,
                    dispatcher, partition, offsetContext, DATABASE_OFFSET);

            assertThat(coordinator.getPendingInheritedTransactions()).isEmpty();
            assertThat(coordinator.getPendingOrphanCommits()).isEmpty();

            // 3 committed events: TX1, TX2, TX3
            verify(dispatcher, times(3)).dispatchTransactionCommittedEvent(any(), any(), any());

            assertThat(processedScn).isNotNull();
        }
        finally {
            simulator.close();
        }
    }

    // ------------------------------------------------------------------
    // Scenario 3: five-archive-logs
    // ------------------------------------------------------------------

    @Test
    void shouldResolveFiveArchiveLogsWithTwoWorkers() throws Exception {
        LogMinerEventSimulator simulator = new LogMinerEventSimulator();
        simulator.loadCsv(getClass().getResourceAsStream("/concurrent/five-archive-logs.csv"));
        List<LogFile> allLogs = simulator.getLogFiles();
        assertThat(allLogs).hasSize(5);

        try (TestableLogMinerWorkerCoordinator coordinator = new TestableLogMinerWorkerCoordinator(
                connectorConfig, schema, JdbcConfiguration.adapt(Configuration.create().build()), metrics, simulator)) {

            assertThat(coordinator.isConcurrentReadingApplicable(allLogs)).isTrue();

            // Phase 1: concurrent wave with 2-log cap.
            // Worker 0 -> a_log1 + a_log2 (TX100, TX101)
            // Worker 1 -> a_log3 + a_log4 (TX102, TX103)
            // a_log5 (TX104) left for next wave.
            List<LogFile> sessionLogs = capLogs(allLogs, connectorConfig.getLogMiningMinimumLogCount());
            Scn processedScn = coordinator.executeWave(
                    Scn.valueOf(0), sessionLogs, dispatcher, partition, offsetContext, DATABASE_OFFSET);

            // No cross-log transactions, so nothing inherited or held back.
            assertThat(coordinator.getPendingInheritedTransactions()).isEmpty();
            assertThat(coordinator.getPendingOrphanCommits()).isEmpty();
            assertThat(coordinator.hasPendingUnknownCommitTransactions()).isFalse();

            // Phase 2: process the remaining logs serially.
            // Passing all logs is safe because the serial worker starts reading from
            // processedScn, so already-processed events are skipped.
            processedScn = coordinator.executeSerial(
                    processedScn, Scn.NULL, allLogs,
                    dispatcher, partition, offsetContext, DATABASE_OFFSET);

            assertThat(coordinator.getPendingInheritedTransactions()).isEmpty();

            // 5 committed events total: TX100, TX101, TX102, TX103, TX104
            verify(dispatcher, times(5)).dispatchTransactionCommittedEvent(any(), any(), any());
            assertThat(processedScn).isNotNull();
        }
        finally {
            simulator.close();
        }
    }

    // ------------------------------------------------------------------ helpers

    private List<LogFile> capLogs(List<LogFile> allLogs, int logsPerThread) {
        long threshold = (long) logsPerThread * SIMULATED_REDO_LOG_SIZE;
        long accumulated = 0;
        List<LogFile> capped = new java.util.ArrayList<>();
        for (LogFile log : allLogs) {
            accumulated += log.getBytes();
            capped.add(log);
            if (accumulated >= threshold) {
                break;
            }
        }
        return capped;
    }
}
