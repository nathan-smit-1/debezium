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
import static org.mockito.Mockito.when;

import java.time.ZoneOffset;
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
 * Unit tests for {@link LogMinerWorkerCoordinator} using a SQLite-based
 * {@link LogMinerEventSimulator} to avoid requiring a live Oracle database.
 *
 * <p>These tests verify that the coordinator correctly plans work units,
 * delegates to simulated workers, and handles bridge units for cross-log
 * transactions.
 *
 * @author Debezium Authors
 */
public class LogMinerWorkerCoordinatorTest {

    private static final ZoneOffset DATABASE_OFFSET = ZoneOffset.UTC;

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
        simulator.loadCsv(getClass().getResourceAsStream("/concurrent/happy-path-template.csv"));

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

    @AfterEach
    void tearDown() throws Exception {
        if (simulator != null) {
            simulator.close();
        }
    }

    /**
     * Tests the classic "happy path" with two archive logs and a cross-log transaction.
     *
     * <p>Transaction 12347 starts in a_log1 and commits in a_log2. With two workers,
     * worker 0 inherits 12347 and worker 1 sees it as an orphan commit. The bridge
     * unit should then resolve it.
     */
    @Test
    void shouldResolveCrossLogTransactionViaBridge() throws Exception {
        List<LogFile> logs = simulator.getLogFiles();
        assertThat(logs).hasSize(2);

        try (TestableLogMinerWorkerCoordinator coordinator = new TestableLogMinerWorkerCoordinator(
                connectorConfig, schema, JdbcConfiguration.adapt(Configuration.create().build()), metrics, simulator)) {

            assertThat(coordinator.isConcurrentReadingApplicable(logs)).isTrue();

            // Execute the main worker wave
            Scn processedScn = coordinator.executeWave(
                    Scn.valueOf(0), logs, dispatcher, partition, offsetContext, DATABASE_OFFSET);

            // After the worker wave, the cross-log transaction should be pending on both sides
            assertThat(coordinator.getPendingInheritedTransactions())
                    .hasSize(1)
                    .anyMatch(tx -> tx.transactionId().equals("12347"));

            assertThat(coordinator.getPendingOrphanCommits())
                    .hasSize(1)
                    .anyMatch(oc -> oc.transactionId().equals("12347"));

            assertThat(coordinator.hasPendingBridgeTransactions()).isTrue();

            // Execute the bridge wave to resolve the cross-log transaction
            Scn bridgeScn = coordinator.executePendingBridges(logs, dispatcher, partition, offsetContext, DATABASE_OFFSET);

            // After the bridge, all cross-log state should be resolved
            assertThat(coordinator.getPendingInheritedTransactions()).isEmpty();
            assertThat(coordinator.getPendingOrphanCommits()).isEmpty();
            assertThat(coordinator.hasPendingBridgeTransactions()).isFalse();

            // Verify that the 3 transactions with DML events dispatched committed events
            verify(dispatcher, times(3)).dispatchTransactionCommittedEvent(any(), any(), any());

            // Heartbeats are dispatched at wave boundaries and for empty transactions
            verify(dispatcher, org.mockito.Mockito.atLeast(1)).dispatchHeartbeatEvent(any(), any());

            // Verify the SCN progressed
            assertThat(processedScn).isNotNull();
            assertThat(bridgeScn).isNotNull();
        }
    }

    /**
     * Verifies that when only one archive log is available, the coordinator falls back
     * to serial processing.
     */
    @Test
    void shouldFallBackToSerialForSingleLog() {
        List<LogFile> singleLog = List.of(
                LogFile.forArchive("a_log1", Scn.valueOf(1), Scn.valueOf(4),
                        java.math.BigInteger.ONE, 1, 1024L, false, false));

        try (TestableLogMinerWorkerCoordinator coordinator = new TestableLogMinerWorkerCoordinator(
                connectorConfig, schema, JdbcConfiguration.adapt(Configuration.create().build()), metrics, simulator)) {

            assertThat(coordinator.isConcurrentReadingApplicable(singleLog)).isFalse();
        }
    }

    /**
     * Verifies that when a redo log is in scope, the coordinator falls back to serial
     * processing.
     */
    @Test
    void shouldFallBackToSerialForRedoLog() {
        List<LogFile> mixedLogs = List.of(
                LogFile.forArchive("a_log1", Scn.valueOf(1), Scn.valueOf(4),
                        java.math.BigInteger.ONE, 1, 1024L, false, false),
                LogFile.forRedo("redo01", Scn.valueOf(4), Scn.valueOf(7),
                        java.math.BigInteger.ONE, false, 1, 1024L));

        try (TestableLogMinerWorkerCoordinator coordinator = new TestableLogMinerWorkerCoordinator(
                connectorConfig, schema, JdbcConfiguration.adapt(Configuration.create().build()), metrics, simulator)) {

            assertThat(coordinator.isConcurrentReadingApplicable(mixedLogs)).isFalse();
        }
    }
}
