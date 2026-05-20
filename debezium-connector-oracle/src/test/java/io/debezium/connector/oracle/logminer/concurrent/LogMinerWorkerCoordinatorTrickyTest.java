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
 * Unit tests for {@link LogMinerWorkerCoordinator} using the tricky-path template.
 *
 * <p>This test exercises a complex 2-worker scenario with multiple cross-log
 * transactions including one with DML spanning logs and one with no DML.
 *
 * @author Debezium Authors
 */
public class LogMinerWorkerCoordinatorTrickyTest {

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
        simulator.loadCsv(getClass().getResourceAsStream("/concurrent/tricky-path-template.csv"));

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

    @Test
    void shouldResolveMultipleCrossLogTransactionsViaBridge() throws Exception {
        List<LogFile> logs = simulator.getLogFiles();
        assertThat(logs).hasSize(3);

        try (TestableLogMinerWorkerCoordinator coordinator = new TestableLogMinerWorkerCoordinator(
                connectorConfig, schema, JdbcConfiguration.adapt(Configuration.create().build()), metrics, simulator)) {

            assertThat(coordinator.isConcurrentReadingApplicable(logs)).isTrue();

            // Execute the main worker wave
            Scn processedScn = coordinator.executeWave(
                    Scn.valueOf(0), logs, dispatcher, partition, offsetContext, DATABASE_OFFSET);

            // Transaction 12349 (START in a_log1, COMMIT in a_log3) should be
            // unresolved / orphaned after the worker wave
            assertThat(coordinator.getPendingInheritedTransactions())
                    .hasSize(1)
                    .anyMatch(tx -> tx.transactionId().equals("12349"));

            assertThat(coordinator.getPendingOrphanCommits())
                    .hasSize(1)
                    .anyMatch(oc -> oc.transactionId().equals("12349"));

            assertThat(coordinator.hasPendingBridgeTransactions()).isTrue();

            // Execute the bridge wave
            Scn bridgeScn = coordinator.executePendingBridges(logs, dispatcher, partition, offsetContext, DATABASE_OFFSET);

            // After bridge everything should be resolved
            assertThat(coordinator.getPendingInheritedTransactions()).isEmpty();
            assertThat(coordinator.getPendingOrphanCommits()).isEmpty();
            assertThat(coordinator.hasPendingBridgeTransactions()).isFalse();

            // 12345, 12346, and 12348 have DML and dispatch committed events.
            // 12347 and 12349 have no DML (START+COMMIT only) so they dispatch heartbeats.
            verify(dispatcher, times(3)).dispatchTransactionCommittedEvent(any(), any(), any());

            // 12349 has no DML so it dispatches heartbeat; wave boundaries also dispatch heartbeats
            verify(dispatcher, org.mockito.Mockito.atLeast(1)).dispatchHeartbeatEvent(any(), any());

            assertThat(processedScn).isNotNull();
            assertThat(bridgeScn).isNotNull();
        }
    }
}
