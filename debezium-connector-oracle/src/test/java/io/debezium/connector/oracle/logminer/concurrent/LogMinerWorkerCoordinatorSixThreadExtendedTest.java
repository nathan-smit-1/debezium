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
 * Unit test for the 6-log extended scenario with 6 concurrent threads.
 *
 * <p>Unlike the 2-thread version where logs 5-6 are never processed concurrently
 * (leaving transaction 10001 unresolved without an orphan commit), the 6-thread
 * version assigns one log per worker.  Worker 5 processes {@code a_log6} and
 * discovers the orphan commit for 10001.  This allows a BRIDGE unit to resolve
 * the cross-log transaction without falling back to serial mode.
 *
 * @author Debezium Authors
 */
public class LogMinerWorkerCoordinatorSixThreadExtendedTest {

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
        simulator.loadCsv(getClass().getResourceAsStream("/concurrent/tricky-path-extended-template.csv"));

        Configuration configuration = Configuration.create()
                .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "logminer")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "server1")
                .with(OracleConnectorConfig.HOSTNAME, "localhost")
                .with(OracleConnectorConfig.DATABASE_NAME, "ORCL")
                .with(OracleConnectorConfig.USER, "debezium")
                // 6 threads means one log per worker for the 6-log set.
                .with(OracleConnectorConfig.LOG_MINING_CONCURRENT_READERS, 6)
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
    void shouldResolveCrossLogTransactionViaBridgeWithSixThreads() throws Exception {
        List<LogFile> logs = simulator.getLogFiles();
        assertThat(logs).hasSize(6);

        try (TestableLogMinerWorkerCoordinator coordinator = new TestableLogMinerWorkerCoordinator(
                connectorConfig, schema, JdbcConfiguration.adapt(Configuration.create().build()), metrics, simulator)) {

            assertThat(coordinator.isConcurrentReadingApplicable(logs)).isTrue();

            // Execute the main worker wave
            Scn processedScn = coordinator.executeWave(
                    Scn.valueOf(0), logs, dispatcher, partition, offsetContext, DATABASE_OFFSET);

            // With 6 threads, worker 5 processes a_log6 and sees the COMMIT for 10001.
            // Because 10001 was started in a_log1 (worker 0), worker 5 produces an orphan commit.
            assertThat(coordinator.getPendingInheritedTransactions())
                    .hasSize(1)
                    .anyMatch(tx -> tx.transactionId().equals("10001"));

            assertThat(coordinator.getPendingOrphanCommits())
                    .hasSize(1)
                    .anyMatch(o -> o.transactionId().equals("10001"));

            // A bridge CAN be planned because inherited + orphan match.
            assertThat(coordinator.hasPendingBridgeTransactions()).isTrue();

            // Execute the bridge follow-up wave.
            Scn bridgeScn = coordinator.executePendingBridges(
                    logs, dispatcher, partition, offsetContext, DATABASE_OFFSET);

            if (!bridgeScn.isNull()) {
                processedScn = bridgeScn;
            }

            // The bridge resolves 10001 but re-discovers 10007 as inherited
            // because 10007 STARTs at SCN 52 (inside bridge range [5,55])
            // but COMMITs at SCN 58 (outside the bridge read end).
            assertThat(coordinator.getPendingInheritedTransactions())
                    .hasSize(1)
                    .anyMatch(tx -> tx.transactionId().equals("10007"));
            assertThat(coordinator.hasPendingUnknownCommitTransactions()).isTrue();

            // Serial fallback over all logs resolves 10007.
            processedScn = coordinator.executeSerial(
                    processedScn, Scn.NULL, logs,
                    dispatcher, partition, offsetContext, DATABASE_OFFSET);

            // After serial wave everything should be resolved.
            assertThat(coordinator.getPendingInheritedTransactions()).isEmpty();
            assertThat(coordinator.getPendingOrphanCommits()).isEmpty();
            assertThat(coordinator.hasPendingUnknownCommitTransactions()).isFalse();
            assertThat(coordinator.hasPendingBridgeTransactions()).isFalse();

            // Total committed events dispatched:
            // - 10002 in concurrent wave (1)
            // - 10003, 10004, 10005, 10006 in bridge wave (4)
            // - 10007 in pre-serial flush from pendingDispatch (1)
            // - 10007 again in serial wave because the simulator lacks the
            // isRecentlyProcessed guard that the real connector has (1)
            // - 10001 has no DML so it dispatches heartbeat, not committed event
            // = 7 committed events total (6 unique + 1 duplicate)
            verify(dispatcher, times(7)).dispatchTransactionCommittedEvent(any(), any(), any());
            verify(dispatcher, org.mockito.Mockito.atLeast(1)).dispatchHeartbeatEvent(any(), any());

            assertThat(processedScn).isNotNull();
        }
    }
}
