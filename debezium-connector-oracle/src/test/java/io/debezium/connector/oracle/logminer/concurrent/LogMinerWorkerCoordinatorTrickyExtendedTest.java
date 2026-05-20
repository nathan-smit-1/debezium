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
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
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
 * Unit tests for {@link LogMinerWorkerCoordinator} using the tricky-path-extended template.
 *
 * <p>This test exercises a 6-log scenario with one massive cross-log transaction
 * spanning all six logs, plus six smaller fully-contained transactions.
 *
 * <p>Unlike the simpler coordinator unit tests, this test emulates the behaviour of
 * the real {@code ConcurrentBufferedLogMinerStreamingChangeEventSource} by applying
 * the same {@code LogFileSessionSelector} capping that happens in production.  The
 * serial fallback therefore receives only a <em>capped</em> subset of logs, and the
 * cap grows by one log per iteration until the cross-log commit is discovered.
 *
 * @author Debezium Authors
 */
public class LogMinerWorkerCoordinatorTrickyExtendedTest {

    private static final ZoneOffset DATABASE_OFFSET = ZoneOffset.UTC;

    /** Simulated redo-log size in bytes; used by the cap calculator. */
    private static final long SIMULATED_REDO_LOG_SIZE = 1024L;

    private LogMinerEventSimulator simulator;
    private OracleConnectorConfig connectorConfig;
    private OracleDatabaseSchema schema;
    private AbstractOracleStreamingChangeEventSourceMetrics metrics;
    private EventDispatcher<OraclePartition, TableId> dispatcher;
    private OraclePartition partition;
    private OracleOffsetContext offsetContext;

    @BeforeEach
    void setUp() throws Exception {
        simulator = new LogMinerEventSimulator();
        simulator.loadCsv(getClass().getResourceAsStream("/concurrent/tricky-path-extended-template.csv"));

        Configuration configuration = Configuration.create()
                .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "logminer")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "server1")
                .with(OracleConnectorConfig.HOSTNAME, "localhost")
                .with(OracleConnectorConfig.DATABASE_NAME, "ORCL")
                .with(OracleConnectorConfig.USER, "debezium")
                // NOTE: For now we are intentionally testing with just 2 concurrent workers.
                // Multi-worker scenarios will be expanded in later tests.
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

    @AfterEach
    void tearDown() throws Exception {
        if (simulator != null) {
            simulator.close();
        }
    }

    @Test
    void shouldResolveSixLogBridgeTransactionWithCappedSession() throws Exception {
        List<LogFile> allLogs = simulator.getLogFiles();
        assertThat(allLogs).hasSize(6);

        try (TestableLogMinerWorkerCoordinator coordinator = new TestableLogMinerWorkerCoordinator(
                connectorConfig, schema, JdbcConfiguration.adapt(Configuration.create().build()), metrics, simulator)) {

            assertThat(coordinator.isConcurrentReadingApplicable(allLogs)).isTrue();

            // ------------------------------------------------------------------
            // Phase 1: concurrent wave over the FIRST capped log subset.
            // CappedLogFileSessionSelector would start with
            // minimumLogsPerRedoThread (2) * redoLogSize (1024) = 2048 bytes,
            // which covers the first two 1024-byte logs.
            // ------------------------------------------------------------------
            int logsPerThread = connectorConfig.getLogMiningMinimumLogCount();
            List<LogFile> sessionLogs = capLogs(allLogs, logsPerThread);
            assertThat(sessionLogs).hasSize(2);

            Scn currentReadScn = Scn.valueOf(0);
            Scn processedScn = coordinator.executeWave(
                    currentReadScn, sessionLogs, dispatcher, partition, offsetContext, DATABASE_OFFSET);

            // After the concurrent wave:
            // - 10002 and 10003 are held back (safe horizon is at SCN 5).
            // - 10001 is inherited (START seen in a_log1, COMMIT not yet found).
            assertThat(coordinator.getPendingInheritedTransactions())
                    .hasSize(1)
                    .anyMatch(tx -> tx.transactionId().equals("10001"));
            assertThat(coordinator.getPendingOrphanCommits()).isEmpty();
            assertThat(coordinator.hasPendingBridgeTransactions()).isFalse();
            assertThat(coordinator.hasPendingUnknownCommitTransactions()).isTrue();

            // ------------------------------------------------------------------
            // Phase 2: serial fallback with a GROWING cap.
            // In production the CappedLogFileSessionSelector grows by one log
            // each iteration while the lower watermark is pinned by the inherited
            // transaction. We emulate that here.
            // ------------------------------------------------------------------
            int serialIterations = 0;
            while (coordinator.hasPendingUnknownCommitTransactions()) {
                logsPerThread++;
                sessionLogs = capLogs(allLogs, logsPerThread);
                serialIterations++;

                processedScn = coordinator.executeSerial(
                        processedScn, Scn.NULL, sessionLogs,
                        dispatcher, partition, offsetContext, DATABASE_OFFSET);
            }

            // The concurrent wave starts with a 2-log cap. Serial fallback then
            // grows the cap by one log each iteration: 3, 4, 5, 6 logs.
            // That is 4 serial iterations until the commit in a_log6 is found.
            assertThat(serialIterations).isEqualTo(4);

            // Everything must be resolved now.
            assertThat(coordinator.getPendingInheritedTransactions()).isEmpty();
            assertThat(coordinator.getPendingOrphanCommits()).isEmpty();
            assertThat(coordinator.hasPendingUnknownCommitTransactions()).isFalse();

            // Total committed events dispatched:
            // - 10002 and 10003 flushed before the first serial wave (2)
            // - 10004 discovered in serial wave 1 (1)
            // - 10005 discovered in serial wave 2 (1)
            // - 10006 discovered in serial wave 3 (1)
            // - 10007 discovered in serial wave 4 (1)
            // - 10001 has no DML so it dispatches a heartbeat, not a committed event
            // = 6 committed events total
            verify(dispatcher, times(6)).dispatchTransactionCommittedEvent(any(), any(), any());
            verify(dispatcher, org.mockito.Mockito.atLeast(1)).dispatchHeartbeatEvent(any(), any());

            assertThat(processedScn).isNotNull();
        }
    }

    /**
     * Simulates {@code CappedLogFileSessionSelector} for a single redo thread.
     *
     * <p>Accumulates logs in sequence order until the total size reaches
     * {@code logsPerThread * SIMULATED_REDO_LOG_SIZE} bytes.
     */
    private List<LogFile> capLogs(List<LogFile> allLogs, int logsPerThread) {
        long threshold = (long) logsPerThread * SIMULATED_REDO_LOG_SIZE;
        long accumulated = 0;
        List<LogFile> capped = new ArrayList<>();
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
