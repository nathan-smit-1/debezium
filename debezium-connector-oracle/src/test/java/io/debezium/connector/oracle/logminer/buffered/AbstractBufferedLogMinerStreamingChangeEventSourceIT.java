/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.logminer.concurrent.ConcurrentBufferedLogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.logminer.concurrent.LogMinerWorker;
import io.debezium.connector.oracle.logminer.concurrent.LogMinerWorkerCoordinator;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Testing;

/**
 * An abstract class for integration tests for {@link BufferedLogMinerStreamingChangeEventSource}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractBufferedLogMinerStreamingChangeEventSourceIT extends AbstractAsyncEngineConnectorTest {

    private OracleConnection connection;

    @BeforeEach
    void before() throws Exception {
        connection = TestHelper.testConnection();
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        TestHelper.dropTable(connection, "dbz3752");

        connection.execute("CREATE TABLE dbz3752(id number(9,0) primary key, name varchar2(50))");
        TestHelper.streamTable(connection, "dbz3752");
    }

    @AfterEach
    void after() throws Exception {
        stopConnector();
        if (connection != null) {
            TestHelper.dropTable(connection, "dbz3752");
            connection.close();
        }
    }

    protected abstract Configuration.Builder getBufferImplementationConfig();

    protected boolean hasPersistedState() {
        return false;
    }

    @Test
    @FixFor("DBZ-3752")
    public void shouldResumeFromPersistedState() throws Exception {
        if (!hasPersistedState()) {
            return;
        }

        // Start the connector using the specified buffer & not to drop the buffer across restarts.
        // The testing framework automatically specifies this as true so we need to override it.
        Configuration config = getBufferImplementationConfig()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, false)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3752")
                .build();

        // Start connector and wait for streaming to begin
        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO dbz3752 (id,name) values (1, 'Mickey Mouse')");

        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.allRecordsInOrder()).hasSize(1);

        List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ3752");
        assertThat(tableRecords).hasSize(1);

        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Mickey Mouse");

        // Stop the connector
        stopConnector();

        connection.execute("INSERT INTO dbz3752 (id,name) values (2, 'Donald Duck')");

        // Restart the connector
        // Upon restart it should rehydrate and begin processing from where it left off.
        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO dbz3752 (id,name) values (3, 'Roger Rabbit')");

        records = consumeRecordsByTopic(2);
        assertThat(records.allRecordsInOrder()).hasSize(2);

        tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ3752");
        assertThat(tableRecords).hasSize(2);

        after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("NAME")).isEqualTo("Donald Duck");

        after = ((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("NAME")).isEqualTo("Roger Rabbit");
    }

    @Test
    @FixFor("DBZ-3752")
    public void shouldResumeLongRunningTransactionFromPersistedState() throws Exception {
        if (!hasPersistedState()) {
            return;
        }

        // Start the connector using the specified buffer & not to drop the buffer across restarts.
        // The testing framework automatically specifies this as true so we need to override it.
        Configuration config = getBufferImplementationConfig()
                .with(OracleConnectorConfig.LOG_MINING_BUFFER_DROP_ON_STOP, false)
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ3752")
                .build();

        // Start connector and wait for streaming to begin
        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Insert two records in two connection, one with a commit and one without.
        try (OracleConnection secondary = TestHelper.testConnection()) {
            connection.executeWithoutCommitting("INSERT INTO dbz3752 (id,name) values (1, 'Mickey Mouse')");
            secondary.execute("INSERT INTO dbz3752 (id,name) values (2, 'Donald Duck')");
        }

        // Get only record
        SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.allRecordsInOrder()).hasSize(1);
        List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ3752");
        assertThat(tableRecords).hasSize(1);

        // Assert record state
        Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("NAME")).isEqualTo("Donald Duck");

        // There should be no more records to consume.
        // The persisted state should contain the Mickey Mouse insert
        assertNoRecordsToConsume();

        // Shutdown the connector
        stopConnector();

        // todo: Verify that (id,name) of (1, 'Mickey Mouse') exists in the persisted data store

        // Add another record while connector off-line
        connection.executeWithoutCommitting("INSERT INTO dbz3752 (id,name) values (3, 'Minnie Mouse')");

        // Restart the connector
        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Add last record after restarting
        connection.execute("INSERT INTO dbz3752 (id,name) values (4, 'Roger Rabbit')");

        // Get records
        records = consumeRecordsByTopic(3);
        assertThat(records.allRecordsInOrder()).hasSize(3);
        tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ3752");
        assertThat(tableRecords).hasSize(3);

        after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Mickey Mouse");

        after = ((Struct) tableRecords.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(3);
        assertThat(after.get("NAME")).isEqualTo("Minnie Mouse");

        after = ((Struct) tableRecords.get(2).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(4);
        assertThat(after.get("NAME")).isEqualTo("Roger Rabbit");
    }

    @Test
    @FixFor("DBZ-8044")
    public void shouldLogAdditionalDetailsForAbandonedTransaction() throws Exception {
        TestHelper.dropTable(connection, "dbz8044");
        try {
            connection.execute("CREATE TABLE dbz8044 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz8044");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8044")
                    .with(OracleConnectorConfig.LOG_MINING_TRANSACTION_RETENTION_MS, "20000")
                    .build();

            final LogInterceptor logInterceptor = new LogInterceptor(BufferedLogMinerStreamingChangeEventSource.class);
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.executeWithoutCommitting("INSERT INTO dbz8044 (id,data) values (1, 'test')");

            Awaitility.await()
                    .atMost(5, TimeUnit.MINUTES)
                    .until(() -> logInterceptor.containsMessage(" is being abandoned"));

            connection.commit();

            assertThat(logInterceptor.containsMessage(String.format(", 1 tables [%s.DEBEZIUM.DBZ8044]", TestHelper.getDatabaseName()))).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz8044");
        }
    }

    @Test
    @FixFor("DBZ-1553")
    public void shouldAdvanceMiningWindowForLongRunningTransaction() throws Exception {
        TestHelper.dropTable(connection, "dbz1553");
        try {
            connection.execute("CREATE TABLE dbz1553 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz1553");

            // Configure the connector with a 30 second window max
            Configuration config = getBufferImplementationConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ1553")
                    .with(OracleConnectorConfig.LOG_MINING_WINDOW_MAX_MS, "30000")
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.NO_DATA)
                    .build();

            final LogInterceptor logInterceptor = new LogInterceptor(BufferedLogMinerStreamingChangeEventSource.class);
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Start a long-running transaction that will not be committed
            connection.executeWithoutCommitting("INSERT INTO dbz1553 (id,data) values (1, 'long-running')");

            // Wait for the window threshold to be exceeded and the mining window to be advanced.
            // The log message should appear once the mining window lower bound is moved past the
            // long-running transaction.
            Awaitility.await()
                    .atMost(Duration.ofMinutes(2))
                    .pollInterval(Duration.ofSeconds(5))
                    .until(() -> logInterceptor.containsWarnMessage("Mining window lower bound advanced"));

            // Verify the warning message indicates the window was advanced due to the threshold
            assertThat(logInterceptor.containsWarnMessage("due to log.mining.window.max.ms threshold")).isTrue();

            // Now commit the long-running transaction
            connection.commit();

            // Consume the record to verify the transaction was fully captured
            SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.allRecordsInOrder()).hasSize(1);

            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ1553");
            assertThat(tableRecords).hasSize(1);

            Struct after = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo("long-running");
        }
        finally {
            TestHelper.dropTable(connection, "dbz1553");
        }
    }

    @Test
    public void shouldProcessEightArchivedLogsWithTwoConcurrentReaders() throws Exception {
        TestHelper.dropTable(connection, "dbz9001");
        try {
            connection.execute("CREATE TABLE dbz9001 (id number(9,0) primary key, name varchar2(50))");
            TestHelper.streamTable(connection, "dbz9001");

            final Configuration config = buildConcurrentArchiveOnlyConfig("DBZ9001", 8).build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            insertCommittedRowsAcrossArchivedLogs("dbz9001", 8, 10, 1);
            TestHelper.forceFlushOfRedoLogsToArchiveLogs();

            final SourceRecords records = consumeRecordsByTopic(80);
            final List<SourceRecord> tableRecords = records.recordsForTopic(topicName("DBZ9001"));
            assertThat(tableRecords).hasSize(80);
            for (int i = 0; i < tableRecords.size(); i++) {
                VerifyRecord.isValidInsert(tableRecords.get(i), "ID", i + 1);
            }
        }
        finally {
            TestHelper.dropTable(connection, "dbz9001");
        }
    }

    @Test
    public void shouldBridgeOpenTransactionCommittedInSecondArchiveLog() throws Exception {
        TestHelper.dropTable(connection, "dbz9002");
        try {
            connection.execute("CREATE TABLE dbz9002 (id number(9,0) primary key, name varchar2(50))");
            TestHelper.streamTable(connection, "dbz9002");

            final Configuration config = buildConcurrentArchiveOnlyConfig("DBZ9002", 2).build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            try (OracleConnection transactionConnection = TestHelper.testConnection()) {
                transactionConnection.executeWithoutCommitting("INSERT INTO dbz9002 (id,name) values (1, 'bridge-in-log2')");
                TestHelper.forceLogfileSwitch();
                transactionConnection.commit();
            }
            TestHelper.forceLogfileSwitch();
            TestHelper.forceFlushOfRedoLogsToArchiveLogs();

            final SourceRecords records = consumeRecordsByTopic(1);
            final List<SourceRecord> tableRecords = records.recordsForTopic(topicName("DBZ9002"));
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidInsert(tableRecords.get(0), "ID", 1);
        }
        finally {
            TestHelper.dropTable(connection, "dbz9002");
        }
    }

    @Test
    public void shouldFallBackToSerialWhenOpenTransactionCommitsInThirdArchiveLog() throws Exception {
        TestHelper.dropTable(connection, "dbz9003");
        try {
            connection.execute("CREATE TABLE dbz9003 (id number(9,0) primary key, name varchar2(50))");
            TestHelper.streamTable(connection, "dbz9003");

            final Configuration config = buildConcurrentArchiveOnlyConfig("DBZ9003", 2).build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            try (OracleConnection transactionConnection = TestHelper.testConnection()) {
                transactionConnection.executeWithoutCommitting("INSERT INTO dbz9003 (id,name) values (1, 'serial-in-log3')");
                TestHelper.forceLogfileSwitch();
                TestHelper.forceLogfileSwitch();
                transactionConnection.commit();
            }
            TestHelper.forceLogfileSwitch();
            TestHelper.forceFlushOfRedoLogsToArchiveLogs();

            final SourceRecords records = consumeRecordsByTopic(1);
            final List<SourceRecord> tableRecords = records.recordsForTopic(topicName("DBZ9003"));
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidInsert(tableRecords.get(0), "ID", 1);
        }
        finally {
            TestHelper.dropTable(connection, "dbz9003");
        }
    }

    @Test
    public void shouldProcessTransactionStartedInFirstLogAndCommittedInThirdLogWithThreeConcurrentReaders() throws Exception {
        TestHelper.dropTable(connection, "dbz9004");
        try {
            connection.execute("CREATE TABLE dbz9004 (id number(9,0) primary key, name varchar2(50))");
            TestHelper.streamTable(connection, "dbz9004");

            final Configuration config = buildConcurrentArchiveOnlyConfig("DBZ9004", 3, 3).build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            try (OracleConnection transactionConnection = TestHelper.testConnection()) {
                transactionConnection.executeWithoutCommitting("INSERT INTO dbz9004 (id,name) values (1, 'commit-in-log3-with-3-readers')");
                TestHelper.forceLogfileSwitch();
                TestHelper.forceLogfileSwitch();
                transactionConnection.commit();
            }
            TestHelper.forceLogfileSwitch();
            TestHelper.forceFlushOfRedoLogsToArchiveLogs();

            final SourceRecords records = consumeRecordsByTopic(1);
            final List<SourceRecord> tableRecords = records.recordsForTopic(topicName("DBZ9004"));
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidInsert(tableRecords.get(0), "ID", 1);
        }
        finally {
            TestHelper.dropTable(connection, "dbz9004");
        }
    }

    @Test
    public void shouldProcessMixedCrossLogTransactionsWithThreeConcurrentReaders() throws Exception {
        TestHelper.dropTable(connection, "dbz9005");
        try {
            connection.execute("CREATE TABLE dbz9005 (id number(9,0) primary key, name varchar2(50))");
            TestHelper.streamTable(connection, "dbz9005");

            final Configuration config = buildConcurrentArchiveOnlyConfig("DBZ9005", 3, 3).build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            try (OracleConnection tx1 = TestHelper.testConnection();
                    OracleConnection tx2 = TestHelper.testConnection();
                    OracleConnection tx3 = TestHelper.testConnection();
                    OracleConnection tx4 = TestHelper.testConnection()) {
                tx1.executeWithoutCommitting("INSERT INTO dbz9005 (id,name) values (1, 'tx1-log1-commit-log2')");
                tx2.executeWithoutCommitting("INSERT INTO dbz9005 (id,name) values (2, 'tx2-log1-commit-log2')");
                tx3.executeWithoutCommitting("INSERT INTO dbz9005 (id,name) values (3, 'tx3-log1-commit-log3')");

                TestHelper.forceLogfileSwitch();

                tx2.commit();
                tx1.commit();
                tx4.executeWithoutCommitting("INSERT INTO dbz9005 (id,name) values (4, 'tx4-log2-commit-log3')");

                TestHelper.forceLogfileSwitch();

                tx3.commit();
                tx4.commit();
            }

            TestHelper.forceLogfileSwitch();
            TestHelper.forceFlushOfRedoLogsToArchiveLogs();

            final SourceRecords records = consumeRecordsByTopic(4);
            final List<SourceRecord> tableRecords = records.recordsForTopic(topicName("DBZ9005"));
            assertThat(tableRecords).hasSize(4);
            VerifyRecord.isValidInsert(tableRecords.get(0), "ID", 2);
            VerifyRecord.isValidInsert(tableRecords.get(1), "ID", 1);
            VerifyRecord.isValidInsert(tableRecords.get(2), "ID", 3);
            VerifyRecord.isValidInsert(tableRecords.get(3), "ID", 4);
        }
        finally {
            TestHelper.dropTable(connection, "dbz9005");
        }
    }

    @Test
    public void shouldProcessChainedNextLogTransactionsWithFourConcurrentReadersUsingBridgeFollowUp() throws Exception {
        TestHelper.dropTable(connection, "dbz9006");
        try {
            connection.execute("CREATE TABLE dbz9006 (id number(9,0) primary key, name varchar2(50))");
            TestHelper.streamTable(connection, "dbz9006");

            final LogInterceptor sourceLogInterceptor = new LogInterceptor(ConcurrentBufferedLogMinerStreamingChangeEventSource.class);
            final LogInterceptor coordinatorLogInterceptor = new LogInterceptor(LogMinerWorkerCoordinator.class);

            final Configuration config = buildConcurrentArchiveOnlyConfig("DBZ9006", 4, 4).build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz9006 (id,name) values (1000, 'seed-offset')");
            connection.execute("COMMIT");
            TestHelper.forceLogfileSwitch();
            TestHelper.forceFlushOfRedoLogsToArchiveLogs();
            final SourceRecords seedRecords = consumeRecordsByTopic(1);
            final List<SourceRecord> seedTableRecords = seedRecords.recordsForTopic(topicName("DBZ9006"));
            assertThat(seedTableRecords).hasSize(1);
            VerifyRecord.isValidInsert(seedTableRecords.get(0), "ID", 1000);

            stopConnector();

            try (OracleConnection tx1 = TestHelper.testConnection();
                    OracleConnection tx2 = TestHelper.testConnection();
                    OracleConnection tx3 = TestHelper.testConnection()) {
                tx1.executeWithoutCommitting("INSERT INTO dbz9006 (id,name) values (1, 'tx1-log1-commit-log2')");

                TestHelper.forceLogfileSwitch();

                tx1.commit();
                tx2.executeWithoutCommitting("INSERT INTO dbz9006 (id,name) values (2, 'tx2-log2-commit-log3')");

                TestHelper.forceLogfileSwitch();

                tx2.commit();
                tx3.executeWithoutCommitting("INSERT INTO dbz9006 (id,name) values (3, 'tx3-log3-commit-log4')");

                TestHelper.forceLogfileSwitch();

                tx3.commit();
            }

            TestHelper.forceLogfileSwitch();
            TestHelper.forceFlushOfRedoLogsToArchiveLogs();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final SourceRecords records = consumeRecordsByTopic(4);
            final List<SourceRecord> tableRecords = records.recordsForTopic(topicName("DBZ9006"));
            assertThat(tableRecords).hasSize(4);
            final List<SourceRecord> scenarioRecords = tableRecords.stream()
                    .filter(record -> ((Number) ((Struct) ((Struct) record.value()).get(Envelope.FieldName.AFTER)).get("ID")).intValue() != 1000)
                    .toList();
            assertThat(scenarioRecords).hasSize(3);
            VerifyRecord.isValidInsert(scenarioRecords.get(0), "ID", 1);
            VerifyRecord.isValidInsert(scenarioRecords.get(1), "ID", 2);
            VerifyRecord.isValidInsert(scenarioRecords.get(2), "ID", 3);

            Awaitility.await().atMost(Duration.ofSeconds(10))
                    .until(() -> sourceLogInterceptor.containsMessage("executing bridge follow-up pass"));
            assertThat(coordinatorLogInterceptor.containsMessage("Planned BRIDGE batch")).isTrue();
            assertThat(coordinatorLogInterceptor.containsMessage("type=BRIDGE")).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz9006");
        }
    }

    @Test
    public void shouldProcessMultipleBridgeableTransactionsInSingleWaveWithFourConcurrentReaders() throws Exception {
        TestHelper.dropTable(connection, "dbz9007");
        try {
            connection.execute("CREATE TABLE dbz9007 (id number(9,0) primary key, name varchar2(50))");
            TestHelper.streamTable(connection, "dbz9007");

            final LogInterceptor sourceLogInterceptor = new LogInterceptor(ConcurrentBufferedLogMinerStreamingChangeEventSource.class);
            final LogInterceptor coordinatorLogInterceptor = new LogInterceptor(LogMinerWorkerCoordinator.class);
            final LogInterceptor workerLogInterceptor = new LogInterceptor(LogMinerWorker.class);

            final Configuration config = buildConcurrentArchiveOnlyConfig("DBZ9007", 4, 4).build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz9007 (id,name) values (1000, 'seed-offset')");
            connection.execute("COMMIT");
            TestHelper.forceLogfileSwitch();
            TestHelper.forceFlushOfRedoLogsToArchiveLogs();
            final SourceRecords seedRecords = consumeRecordsByTopic(1);
            final List<SourceRecord> seedTableRecords = seedRecords.recordsForTopic(topicName("DBZ9007"));
            assertThat(seedTableRecords).hasSize(1);
            VerifyRecord.isValidInsert(seedTableRecords.get(0), "ID", 1000);

            stopConnector();

            try (OracleConnection tx1 = TestHelper.testConnection();
                    OracleConnection tx2 = TestHelper.testConnection();
                    OracleConnection tx3 = TestHelper.testConnection()) {
                tx1.executeWithoutCommitting("INSERT INTO dbz9007 (id,name) values (1, 'tx1-log1-commit-log2')");

                TestHelper.forceLogfileSwitch();

                tx1.commit();
                tx2.executeWithoutCommitting("INSERT INTO dbz9007 (id,name) values (2, 'tx2-log2-commit-log3')");
                tx3.executeWithoutCommitting("INSERT INTO dbz9007 (id,name) values (3, 'tx3-log2-commit-log3')");

                TestHelper.forceLogfileSwitch();

                tx2.commit();
                tx3.commit();
            }

            TestHelper.forceLogfileSwitch();
            TestHelper.forceFlushOfRedoLogsToArchiveLogs();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final SourceRecords records = consumeRecordsByTopic(4);
            final List<SourceRecord> tableRecords = records.recordsForTopic(topicName("DBZ9007"));
            assertThat(tableRecords).hasSize(4);
            final List<SourceRecord> scenarioRecords = tableRecords.stream()
                    .filter(record -> ((Number) ((Struct) ((Struct) record.value()).get(Envelope.FieldName.AFTER)).get("ID")).intValue() != 1000)
                    .toList();
            assertThat(scenarioRecords).hasSize(3);
            VerifyRecord.isValidInsert(scenarioRecords.get(0), "ID", 1);
            VerifyRecord.isValidInsert(scenarioRecords.get(1), "ID", 2);
            VerifyRecord.isValidInsert(scenarioRecords.get(2), "ID", 3);

            Awaitility.await().atMost(Duration.ofSeconds(10))
                    .until(() -> sourceLogInterceptor.containsMessage("executing bridge follow-up pass"));
            assertThat(coordinatorLogInterceptor.containsMessage("Planned BRIDGE batch")).isTrue();
            assertThat(workerLogInterceptor.containsMessage("type=BRIDGE, resolved=2")).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz9007");
        }
    }

    @Test
    public void shouldProcessMultipleDistinctBridgeBatchesInSingleWaveWithFourConcurrentReaders() throws Exception {
        TestHelper.dropTable(connection, "dbz9008");
        try {
            connection.execute("CREATE TABLE dbz9008 (id number(9,0) primary key, name varchar2(50))");
            TestHelper.streamTable(connection, "dbz9008");

            final LogInterceptor sourceLogInterceptor = new LogInterceptor(ConcurrentBufferedLogMinerStreamingChangeEventSource.class);
            final LogInterceptor coordinatorLogInterceptor = new LogInterceptor(LogMinerWorkerCoordinator.class);
            final LogInterceptor workerLogInterceptor = new LogInterceptor(LogMinerWorker.class);

            final Configuration config = buildConcurrentArchiveOnlyConfig("DBZ9008", 4, 4).build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz9008 (id,name) values (1000, 'seed-offset')");
            connection.execute("COMMIT");
            TestHelper.forceLogfileSwitch();
            TestHelper.forceFlushOfRedoLogsToArchiveLogs();
            final SourceRecords seedRecords = consumeRecordsByTopic(1);
            final List<SourceRecord> seedTableRecords = seedRecords.recordsForTopic(topicName("DBZ9008"));
            assertThat(seedTableRecords).hasSize(1);
            VerifyRecord.isValidInsert(seedTableRecords.get(0), "ID", 1000);

            stopConnector();

            TestHelper.forceLogfileSwitch();
            TestHelper.forceFlushOfRedoLogsToArchiveLogs();

            try (OracleConnection tx1 = TestHelper.testConnection();
                    OracleConnection tx2 = TestHelper.testConnection();
                    OracleConnection tx3 = TestHelper.testConnection()) {
                tx1.executeWithoutCommitting("INSERT INTO dbz9008 (id,name) values (1, 'tx1-early-crossing')");

                TestHelper.forceLogfileSwitch();

                connection.execute("INSERT INTO dbz9008 (id,name) values (101, 'filler-mid-wave')");
                connection.execute("COMMIT");

                TestHelper.forceLogfileSwitch();

                tx2.executeWithoutCommitting("INSERT INTO dbz9008 (id,name) values (2, 'tx2-late-crossing')");
                tx3.executeWithoutCommitting("INSERT INTO dbz9008 (id,name) values (3, 'tx3-late-crossing')");

                TestHelper.forceLogfileSwitch();

                connection.execute("INSERT INTO dbz9008 (id,name) values (102, 'filler-late-wave')");
                connection.execute("COMMIT");

                TestHelper.forceLogfileSwitch();

                tx1.commit();
                tx2.commit();
                tx3.commit();
            }

            TestHelper.forceLogfileSwitch();
            TestHelper.forceFlushOfRedoLogsToArchiveLogs();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            Awaitility.await().atMost(Duration.ofSeconds(30))
                    .until(() -> coordinatorLogInterceptor.countOccurrences("Planned BRIDGE batch") >= 2);

            final SourceRecords records = consumeRecordsByTopic(6);
            final List<SourceRecord> tableRecords = records.recordsForTopic(topicName("DBZ9008"));
            assertThat(tableRecords.size()).isBetween(5, 6);
            final List<SourceRecord> scenarioRecords = tableRecords.stream()
                    .filter(record -> {
                        final int id = ((Number) ((Struct) ((Struct) record.value()).get(Envelope.FieldName.AFTER)).get("ID")).intValue();
                        return id != 1000;
                    })
                    .toList();
            assertThat(scenarioRecords).hasSize(5);
            assertThat(scenarioRecords)
                    .extracting(record -> ((Number) ((Struct) ((Struct) record.value()).get(Envelope.FieldName.AFTER)).get("ID")).intValue())
                    .containsExactlyInAnyOrder(1, 2, 3, 101, 102);

            assertThat(sourceLogInterceptor.containsMessage("executing bridge follow-up pass")).isTrue();
            assertThat(coordinatorLogInterceptor.countOccurrences("Planned BRIDGE batch")).isGreaterThanOrEqualTo(2);
            assertThat(workerLogInterceptor.containsMessage("type=BRIDGE")).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz9008");
        }
    }

    @Test
    public void shouldProcessReplayTailAfterBridgeFollowUpWithTwoConcurrentReaders() throws Exception {
        TestHelper.dropTable(connection, "dbz9009");
        try {
            connection.execute("CREATE TABLE dbz9009 (id number(9,0) primary key, name varchar2(50))");
            TestHelper.streamTable(connection, "dbz9009");

            final LogInterceptor sourceLogInterceptor = new LogInterceptor(ConcurrentBufferedLogMinerStreamingChangeEventSource.class);
            final LogInterceptor coordinatorLogInterceptor = new LogInterceptor(LogMinerWorkerCoordinator.class);

            final Configuration config = buildConcurrentArchiveOnlyConfig("DBZ9009", 4, 2).build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz9009 (id,name) values (1000, 'seed-offset')");
            connection.execute("COMMIT");
            TestHelper.forceLogfileSwitch();
            TestHelper.forceFlushOfRedoLogsToArchiveLogs();
            final SourceRecords seedRecords = consumeRecordsByTopic(1);
            final List<SourceRecord> seedTableRecords = seedRecords.recordsForTopic(topicName("DBZ9009"));
            assertThat(seedTableRecords).hasSize(1);
            VerifyRecord.isValidInsert(seedTableRecords.get(0), "ID", 1000);

            stopConnector();

            try (OracleConnection tx1 = TestHelper.testConnection()) {
                tx1.executeWithoutCommitting("INSERT INTO dbz9009 (id,name) values (1, 'bridge-then-replay-bridge')");

                TestHelper.forceLogfileSwitch();
                TestHelper.forceLogfileSwitch();

                tx1.commit();

                TestHelper.forceLogfileSwitch();

                connection.execute("INSERT INTO dbz9009 (id,name) values (2, 'bridge-then-replay-tail')");
                connection.execute("COMMIT");
            }

            TestHelper.forceLogfileSwitch();
            TestHelper.forceFlushOfRedoLogsToArchiveLogs();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final SourceRecords records = consumeRecordsByTopic(3);
            final List<SourceRecord> tableRecords = records.recordsForTopic(topicName("DBZ9009"));
            assertThat(tableRecords).hasSize(3);
            final List<SourceRecord> scenarioRecords = tableRecords.stream()
                    .filter(record -> ((Number) ((Struct) ((Struct) record.value()).get(Envelope.FieldName.AFTER)).get("ID")).intValue() != 1000)
                    .toList();
            assertThat(scenarioRecords).hasSize(2);
            VerifyRecord.isValidInsert(scenarioRecords.get(0), "ID", 1);
            VerifyRecord.isValidInsert(scenarioRecords.get(1), "ID", 2);

            Awaitility.await().atMost(Duration.ofSeconds(10))
                    .until(() -> sourceLogInterceptor.containsMessage("executing bridge follow-up pass"));
            Awaitility.await().atMost(Duration.ofSeconds(10))
                    .until(() -> sourceLogInterceptor.containsMessage("executing replay follow-up pass"));
            assertThat(coordinatorLogInterceptor.containsMessage("Planned BRIDGE batch")).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz9009");
        }
    }

    private Configuration.Builder buildConcurrentArchiveOnlyConfig(String tableName, int minimumLogCount) {
        return buildConcurrentArchiveOnlyConfig(tableName, minimumLogCount, 2);
    }

    private Configuration.Builder buildConcurrentArchiveOnlyConfig(String tableName, int minimumLogCount, int readerCount) {
        return getBufferImplementationConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\." + tableName)
                .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.NO_DATA)
                .with(OracleConnectorConfig.LOG_MINING_ARCHIVE_LOG_ONLY_MODE, true)
                .with(OracleConnectorConfig.LOG_MINING_CONCURRENT_READERS, readerCount)
                .with(OracleConnectorConfig.LOG_MINING_LOG_COUNT_MIN, minimumLogCount);
    }

    private void insertCommittedRowsAcrossArchivedLogs(String tableName, int archiveLogs, int rowsPerLog, int startingId) throws Exception {
        int nextId = startingId;
        for (int logIndex = 0; logIndex < archiveLogs; logIndex++) {
            for (int rowIndex = 0; rowIndex < rowsPerLog; rowIndex++) {
                connection.execute(String.format("INSERT INTO %s (id,name) values (%d, 'name-%d')", tableName, nextId, nextId));
                nextId++;
            }
            TestHelper.forceLogfileSwitch();
        }
    }

    private String topicName(String tableName) {
        return "server1.DEBEZIUM." + tableName;
    }
}
