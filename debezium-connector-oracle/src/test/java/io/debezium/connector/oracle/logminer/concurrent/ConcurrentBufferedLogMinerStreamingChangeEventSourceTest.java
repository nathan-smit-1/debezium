/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleDatabaseVersion;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogFile;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

class ConcurrentBufferedLogMinerStreamingChangeEventSourceTest {

    private static final Scn SCN_100 = Scn.valueOf(100);
    private static final Scn SCN_300 = Scn.valueOf(300);
    private static final Scn SCN_320 = Scn.valueOf(320);
    private static final Scn SCN_340 = Scn.valueOf(340);
    private static final Scn SCN_350 = Scn.valueOf(350);
    private static final Scn SCN_360 = Scn.valueOf(360);
    private static final Scn SCN_450 = Scn.valueOf(450);
    private static final Scn SCN_500 = Scn.valueOf(500);
    private static final Scn SCN_600 = Scn.valueOf(600);

    @Test
    void testArchiveRedoArchiveTransitionsBetweenConcurrentAndSerialModes() throws Exception {
        final ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true, true, true, false);
        when(context.isPaused()).thenReturn(false);

        final EventDispatcher<OraclePartition, TableId> dispatcher = mockDispatcher();
        final OraclePartition partition = mock(OraclePartition.class);
        final OracleOffsetContext offsetContext = mockOffsetContext(SCN_100);
        final LogMinerStreamingChangeEventSourceMetrics metrics = mockMetrics();
        final LogMinerWorkerCoordinator coordinator = mock(LogMinerWorkerCoordinator.class);

        when(coordinator.hasPendingUnknownCommitTransactions()).thenReturn(false, false, false);
        when(coordinator.executeWave(eq(SCN_100), any(), eq(dispatcher), eq(partition), eq(offsetContext), eq(ZoneOffset.UTC)))
                .thenReturn(SCN_300);
        when(coordinator.executeWave(eq(SCN_450), any(), eq(dispatcher), eq(partition), eq(offsetContext), eq(ZoneOffset.UTC)))
                .thenReturn(SCN_600);
        when(coordinator.executeSerial(eq(SCN_300), eq(SCN_350), any(), eq(dispatcher), eq(partition), eq(offsetContext), eq(ZoneOffset.UTC)))
                .thenReturn(SCN_350);
        when(coordinator.executeSerial(eq(SCN_350), eq(SCN_450), any(), eq(dispatcher), eq(partition), eq(offsetContext), eq(ZoneOffset.UTC)))
                .thenReturn(SCN_450);

        final TestStreamingSource source = createSource(context, dispatcher, partition, offsetContext, metrics, coordinator);
        source.enqueueWindow(SCN_300, SCN_350, List.of(
                archiveLog("arc1", SCN_100, Scn.valueOf(200), 1),
                archiveLog("arc2", Scn.valueOf(200), SCN_300, 2),
                redoLog("redo1", SCN_300, Scn.valueOf(400), 3)));
        source.enqueueWindow(SCN_450, SCN_450, List.of(
                archiveLog("arc3", SCN_350, Scn.valueOf(400), 4),
                redoLog("redo2", Scn.valueOf(400), SCN_450, 5)));
        source.enqueueWindow(SCN_600, SCN_600, List.of(
                archiveLog("arc4", SCN_450, Scn.valueOf(520), 6),
                archiveLog("arc5", Scn.valueOf(520), SCN_600, 7)));

        source.runLoop();

        final InOrder order = inOrder(coordinator, offsetContext);
        order.verify(coordinator).executeWave(eq(SCN_100), argThat(logsNamed("arc1", "arc2")), eq(dispatcher), eq(partition), eq(offsetContext),
                eq(ZoneOffset.UTC));
        order.verify(coordinator).executeSerial(eq(SCN_300), eq(SCN_350), argThat(logsNamed("redo1")), eq(dispatcher), eq(partition),
                eq(offsetContext), eq(ZoneOffset.UTC));
        order.verify(offsetContext).setScn(SCN_350);
        order.verify(coordinator).executeSerial(eq(SCN_350), eq(SCN_450), argThat(logsNamed("arc3", "redo2")), eq(dispatcher), eq(partition),
                eq(offsetContext), eq(ZoneOffset.UTC));
        order.verify(offsetContext).setScn(SCN_450);
        order.verify(coordinator).executeWave(eq(SCN_450), argThat(logsNamed("arc4", "arc5")), eq(dispatcher), eq(partition), eq(offsetContext),
                eq(ZoneOffset.UTC));
        order.verify(offsetContext).setScn(SCN_600);
    }

    @Test
    void testUnknownCommitStateForcesSerialFallbackThenConcurrentResumes() throws Exception {
        final ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true, true, false);
        when(context.isPaused()).thenReturn(false);

        final EventDispatcher<OraclePartition, TableId> dispatcher = mockDispatcher();
        final OraclePartition partition = mock(OraclePartition.class);
        final OracleOffsetContext offsetContext = mockOffsetContext(SCN_100);
        final LogMinerStreamingChangeEventSourceMetrics metrics = mockMetrics();
        final LogMinerWorkerCoordinator coordinator = mock(LogMinerWorkerCoordinator.class);

        when(coordinator.hasPendingUnknownCommitTransactions()).thenReturn(true, false);
        when(coordinator.executeSerial(eq(SCN_100), eq(SCN_300), any(), eq(dispatcher), eq(partition), eq(offsetContext), eq(ZoneOffset.UTC)))
                .thenReturn(SCN_300);
        when(coordinator.executeWave(eq(SCN_300), any(), eq(dispatcher), eq(partition), eq(offsetContext), eq(ZoneOffset.UTC)))
                .thenReturn(SCN_500);

        final TestStreamingSource source = createSource(context, dispatcher, partition, offsetContext, metrics, coordinator);
        source.enqueueWindow(SCN_300, SCN_300, List.of(
                archiveLog("arc1", SCN_100, Scn.valueOf(200), 1),
                archiveLog("arc2", Scn.valueOf(200), SCN_300, 2)));
        source.enqueueWindow(SCN_500, SCN_500, List.of(
                archiveLog("arc3", SCN_300, Scn.valueOf(400), 3),
                archiveLog("arc4", Scn.valueOf(400), SCN_500, 4)));

        source.runLoop();

        final InOrder order = inOrder(coordinator, offsetContext);
        order.verify(coordinator).executeSerial(eq(SCN_100), eq(SCN_300), argThat(logsNamed("arc1", "arc2")), eq(dispatcher), eq(partition),
                eq(offsetContext), eq(ZoneOffset.UTC));
        order.verify(offsetContext).setScn(SCN_300);
        order.verify(coordinator).executeWave(eq(SCN_300), argThat(logsNamed("arc3", "arc4")), eq(dispatcher), eq(partition), eq(offsetContext),
                eq(ZoneOffset.UTC));
        order.verify(offsetContext).setScn(SCN_500);
    }

    @Test
    void testBridgeAndReplayRunBeforeRedoSerialFollowUp() throws Exception {
        final ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true, false);
        when(context.isPaused()).thenReturn(false);

        final EventDispatcher<OraclePartition, TableId> dispatcher = mockDispatcher();
        final OraclePartition partition = mock(OraclePartition.class);
        final OracleOffsetContext offsetContext = mockOffsetContext(SCN_100);
        final LogMinerStreamingChangeEventSourceMetrics metrics = mockMetrics();
        final LogMinerWorkerCoordinator coordinator = mock(LogMinerWorkerCoordinator.class);

        when(coordinator.hasPendingUnknownCommitTransactions()).thenReturn(false);
        when(coordinator.executeWave(eq(SCN_100), any(), eq(dispatcher), eq(partition), eq(offsetContext), eq(ZoneOffset.UTC)))
                .thenReturn(SCN_300);
        when(coordinator.hasPendingBridgeTransactions()).thenReturn(true, true, false, false, false);
        when(coordinator.hasPendingReplayUnits()).thenReturn(true, true, false);
        when(coordinator.executePendingBridges(any(), eq(dispatcher), eq(partition), eq(offsetContext), eq(ZoneOffset.UTC)))
                .thenReturn(SCN_320);
        when(coordinator.executePendingReplays(any(), eq(dispatcher), eq(partition), eq(offsetContext), eq(ZoneOffset.UTC)))
                .thenReturn(SCN_340);
        when(coordinator.executeSerial(eq(SCN_340), eq(SCN_350), any(), eq(dispatcher), eq(partition), eq(offsetContext), eq(ZoneOffset.UTC)))
                .thenReturn(SCN_360);

        final TestStreamingSource source = createSource(context, dispatcher, partition, offsetContext, metrics, coordinator);
        source.enqueueWindow(SCN_300, SCN_350, List.of(
                archiveLog("arc1", SCN_100, Scn.valueOf(200), 1),
                archiveLog("arc2", Scn.valueOf(200), SCN_300, 2),
                redoLog("redo1", SCN_300, Scn.valueOf(400), 3)));

        source.runLoop();

        final InOrder order = inOrder(coordinator, offsetContext);
        order.verify(coordinator).executeWave(eq(SCN_100), argThat(logsNamed("arc1", "arc2")), eq(dispatcher), eq(partition), eq(offsetContext),
                eq(ZoneOffset.UTC));
        order.verify(coordinator).executePendingBridges(argThat(logsNamed("arc1", "arc2")), eq(dispatcher), eq(partition), eq(offsetContext),
                eq(ZoneOffset.UTC));
        order.verify(coordinator).executePendingReplays(argThat(logsNamed("arc1", "arc2")), eq(dispatcher), eq(partition), eq(offsetContext),
                eq(ZoneOffset.UTC));
        order.verify(coordinator).executeSerial(eq(SCN_340), eq(SCN_350), argThat(logsNamed("redo1")), eq(dispatcher), eq(partition),
                eq(offsetContext), eq(ZoneOffset.UTC));
        order.verify(offsetContext).setScn(SCN_360);
    }

    @Test
    void testUnknownCommitAfterConcurrentWaveUsesSerialContinuationAcrossAllLogs() throws Exception {
        final ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true, false);
        when(context.isPaused()).thenReturn(false);

        final EventDispatcher<OraclePartition, TableId> dispatcher = mockDispatcher();
        final OraclePartition partition = mock(OraclePartition.class);
        final OracleOffsetContext offsetContext = mockOffsetContext(SCN_100);
        final LogMinerStreamingChangeEventSourceMetrics metrics = mockMetrics();
        final LogMinerWorkerCoordinator coordinator = mock(LogMinerWorkerCoordinator.class);

        when(coordinator.hasPendingUnknownCommitTransactions()).thenReturn(false, true);
        when(coordinator.getPendingInheritedTransactions()).thenReturn(List.of(inheritedTransaction("tx-a", SCN_100)));
        when(coordinator.executeWave(eq(SCN_100), any(), eq(dispatcher), eq(partition), eq(offsetContext), eq(ZoneOffset.UTC)))
                .thenReturn(SCN_300);
        when(coordinator.executeSerial(eq(SCN_300), eq(SCN_350), any(), eq(dispatcher), eq(partition), eq(offsetContext), eq(ZoneOffset.UTC)))
                .thenReturn(SCN_350);

        final TestStreamingSource source = createSource(context, dispatcher, partition, offsetContext, metrics, coordinator);
        source.enqueueWindow(SCN_300, SCN_350, List.of(
                archiveLog("arc1", SCN_100, Scn.valueOf(200), 1),
                archiveLog("arc2", Scn.valueOf(200), SCN_300, 2),
                redoLog("redo1", SCN_300, Scn.valueOf(400), 3)));

        source.runLoop();

        final InOrder order = inOrder(coordinator, offsetContext);
        order.verify(coordinator).executeWave(eq(SCN_100), argThat(logsNamed("arc1", "arc2")), eq(dispatcher), eq(partition), eq(offsetContext),
                eq(ZoneOffset.UTC));
        order.verify(coordinator).executeSerial(eq(SCN_300), eq(SCN_350), argThat(logsNamed("arc1", "arc2", "redo1")), eq(dispatcher),
                eq(partition), eq(offsetContext), eq(ZoneOffset.UTC));
        order.verify(offsetContext).setScn(SCN_350);
    }

    @Test
    void testUnknownCommitFallbackCollectsLogsFromOldestInheritedStart() throws Exception {
        final ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true, false);
        when(context.isPaused()).thenReturn(false);

        final EventDispatcher<OraclePartition, TableId> dispatcher = mockDispatcher();
        final OraclePartition partition = mock(OraclePartition.class);
        final OracleOffsetContext offsetContext = mockOffsetContext(SCN_300);
        final LogMinerStreamingChangeEventSourceMetrics metrics = mockMetrics();
        final LogMinerWorkerCoordinator coordinator = mock(LogMinerWorkerCoordinator.class);

        when(coordinator.hasPendingUnknownCommitTransactions()).thenReturn(true);
        when(coordinator.getPendingInheritedTransactions()).thenReturn(List.of(inheritedTransaction("tx-a", SCN_100)));
        when(coordinator.executeSerial(eq(SCN_300), eq(SCN_350), any(), eq(dispatcher), eq(partition), eq(offsetContext), eq(ZoneOffset.UTC)))
                .thenReturn(SCN_350);

        final TestStreamingSource source = createSource(context, dispatcher, partition, offsetContext, metrics, coordinator);
        source.enqueueWindow(SCN_350, SCN_350, List.of(
                archiveLog("arc1", SCN_100, Scn.valueOf(200), 1),
                archiveLog("arc2", Scn.valueOf(200), SCN_300, 2),
                redoLog("redo1", SCN_300, Scn.valueOf(400), 3)));

        source.runLoop();

        assertThat(source.getRequestedLogCollectionStarts()).containsExactly(SCN_100.subtract(Scn.ONE));
        verify(coordinator).executeSerial(eq(SCN_300), eq(SCN_350), argThat(logsNamed("arc1", "arc2", "redo1")), eq(dispatcher),
                eq(partition), eq(offsetContext), eq(ZoneOffset.UTC));
    }

    @Test
    void testRedoOnlyWaveAlwaysFallsBackToSerial() throws Exception {
        final ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true, false);
        when(context.isPaused()).thenReturn(false);

        final EventDispatcher<OraclePartition, TableId> dispatcher = mockDispatcher();
        final OraclePartition partition = mock(OraclePartition.class);
        final OracleOffsetContext offsetContext = mockOffsetContext(SCN_100);
        final LogMinerStreamingChangeEventSourceMetrics metrics = mockMetrics();
        final LogMinerWorkerCoordinator coordinator = mock(LogMinerWorkerCoordinator.class);

        when(coordinator.hasPendingUnknownCommitTransactions()).thenReturn(false);
        when(coordinator.executeSerial(eq(SCN_100), eq(SCN_350), any(), eq(dispatcher), eq(partition), eq(offsetContext), eq(ZoneOffset.UTC)))
                .thenReturn(SCN_350);

        final TestStreamingSource source = createSource(context, dispatcher, partition, offsetContext, metrics, coordinator);
        source.enqueueWindow(SCN_350, SCN_350, List.of(
                redoLog("redo1", SCN_100, Scn.valueOf(200), 1),
                redoLog("redo2", Scn.valueOf(200), SCN_350, 2)));

        source.runLoop();

        verify(coordinator, never()).executeWave(any(), any(), any(), any(), any(), any());
        verify(coordinator).executeSerial(eq(SCN_100), eq(SCN_350), argThat(logsNamed("redo1", "redo2")), eq(dispatcher), eq(partition),
                eq(offsetContext), eq(ZoneOffset.UTC));
        verify(offsetContext).setScn(SCN_350);
    }

    @Test
    void testRedoOnlySerialWavesAdvanceReadWindowAcrossSameRedoLog() throws Exception {
        final ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true, true, false);
        when(context.isPaused()).thenReturn(false);

        final EventDispatcher<OraclePartition, TableId> dispatcher = mockDispatcher();
        final OraclePartition partition = mock(OraclePartition.class);
        final OracleOffsetContext offsetContext = mockOffsetContext(SCN_300);
        final LogMinerStreamingChangeEventSourceMetrics metrics = mockMetrics();
        final LogMinerWorkerCoordinator coordinator = mock(LogMinerWorkerCoordinator.class);

        when(coordinator.hasPendingUnknownCommitTransactions()).thenReturn(false, false);
        when(coordinator.executeSerial(eq(SCN_300), eq(SCN_350), any(), eq(dispatcher), eq(partition), eq(offsetContext), eq(ZoneOffset.UTC)))
                .thenReturn(SCN_350);
        when(coordinator.executeSerial(eq(SCN_350), eq(SCN_450), any(), eq(dispatcher), eq(partition), eq(offsetContext), eq(ZoneOffset.UTC)))
                .thenReturn(SCN_450);

        final TestStreamingSource source = createSource(context, dispatcher, partition, offsetContext, metrics, coordinator);
        source.enqueueWindow(SCN_350, SCN_350, List.of(
                redoLog("redo1", SCN_100, Scn.valueOf(500), 1)));
        source.enqueueWindow(SCN_450, SCN_450, List.of(
                redoLog("redo1", SCN_100, Scn.valueOf(500), 1)));

        source.runLoop();

        final InOrder order = inOrder(coordinator, offsetContext);
        order.verify(coordinator).executeSerial(eq(SCN_300), eq(SCN_350), argThat(logsNamed("redo1")), eq(dispatcher), eq(partition),
                eq(offsetContext), eq(ZoneOffset.UTC));
        order.verify(offsetContext).setScn(SCN_350);
        order.verify(coordinator).executeSerial(eq(SCN_350), eq(SCN_450), argThat(logsNamed("redo1")), eq(dispatcher), eq(partition),
                eq(offsetContext), eq(ZoneOffset.UTC));
        order.verify(offsetContext).setScn(SCN_450);
    }

    private TestStreamingSource createSource(ChangeEventSourceContext context,
                                             EventDispatcher<OraclePartition, TableId> dispatcher,
                                             OraclePartition partition,
                                             OracleOffsetContext offsetContext,
                                             LogMinerStreamingChangeEventSourceMetrics metrics,
                                             LogMinerWorkerCoordinator coordinator)
            throws Exception {
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(
                TestHelper.defaultConfig()
                        .with(OracleConnectorConfig.ARCHIVE_DESTINATION_NAME, "LOG_ARCHIVE_DEST_1")
                        .with(OracleConnectorConfig.LOG_MINING_CONCURRENT_READERS, 2)
                        .build());

        final OracleConnection connection = mock(OracleConnection.class);
        final OracleDatabaseVersion version = mock(OracleDatabaseVersion.class);
        when(version.getMajor()).thenReturn(19);
        when(connection.getOracleVersion()).thenReturn(version);
        when(connection.isArchiveLogDestinationValid(anyString())).thenReturn(true);

        final TestStreamingSource source = new TestStreamingSource(
                connectorConfig,
                connection,
                dispatcher,
                mock(ErrorHandler.class),
                mock(OracleDatabaseSchema.class),
                metrics,
                context,
                partition,
                offsetContext);

        final Field coordinatorField = ConcurrentBufferedLogMinerStreamingChangeEventSource.class.getDeclaredField("coordinator");
        coordinatorField.setAccessible(true);
        coordinatorField.set(source, coordinator);
        return source;
    }

    @SuppressWarnings("unchecked")
    private EventDispatcher<OraclePartition, TableId> mockDispatcher() {
        return mock(EventDispatcher.class);
    }

    private OracleOffsetContext mockOffsetContext(Scn startScn) {
        final OracleOffsetContext offsetContext = mock(OracleOffsetContext.class);
        when(offsetContext.getScn()).thenReturn(startScn);
        when(offsetContext.getCommitScn()).thenReturn(CommitScn.valueOf((String) null));
        return offsetContext;
    }

    private LogMinerStreamingChangeEventSourceMetrics mockMetrics() {
        final LogMinerStreamingChangeEventSourceMetrics metrics = mock(LogMinerStreamingChangeEventSourceMetrics.class);
        when(metrics.getDatabaseOffset()).thenReturn(ZoneOffset.UTC);
        return metrics;
    }

    private static org.mockito.ArgumentMatcher<List<LogFile>> logsNamed(String... names) {
        final List<String> expected = List.of(names);
        return logs -> logs != null && logs.stream().map(LogFile::getFileName).toList().equals(expected);
    }

    private static LogFile archiveLog(String name, Scn firstScn, Scn nextScn, int seq) {
        return LogFile.forArchive(name, firstScn, nextScn, BigInteger.valueOf(seq), 1, 1024L, false, false);
    }

    private static LogFile redoLog(String name, Scn firstScn, Scn nextScn, int seq) {
        return LogFile.forRedo(name, firstScn, nextScn, BigInteger.valueOf(seq), false, 1, 1024L);
    }

    private static InheritedTransaction inheritedTransaction(String transactionId, Scn startScn) {
        return new InheritedTransaction(transactionId, startScn, Instant.EPOCH, "user", "client", 1, startScn, startScn, List.of());
    }

    private static class TestStreamingSource extends ConcurrentBufferedLogMinerStreamingChangeEventSource {

        private final ChangeEventSourceContext context;
        private final OraclePartition partition;
        private final OracleOffsetContext offsetContext;
        private final Queue<Scn> requestedLogCollectionStarts = new ArrayDeque<>();
        private final Queue<WindowInput> windows = new ArrayDeque<>();

        private TestStreamingSource(OracleConnectorConfig connectorConfig,
                                    OracleConnection jdbcConnection,
                                    EventDispatcher<OraclePartition, TableId> dispatcher,
                                    ErrorHandler errorHandler,
                                    OracleDatabaseSchema schema,
                                    LogMinerStreamingChangeEventSourceMetrics metrics,
                                    ChangeEventSourceContext context,
                                    OraclePartition partition,
                                    OracleOffsetContext offsetContext) {
            super(connectorConfig, jdbcConnection, dispatcher, errorHandler, Clock.SYSTEM, schema, connectorConfig.getJdbcConfig(), metrics);
            this.context = context;
            this.partition = partition;
            this.offsetContext = offsetContext;
        }

        void enqueueWindow(Scn upperBoundScn, Scn finalUpperBoundScn, List<LogFile> logFiles) {
            windows.add(new WindowInput(upperBoundScn, finalUpperBoundScn, logFiles));
        }

        void runLoop() throws Exception {
            executeLogMiningStreaming();
        }

        List<Scn> getRequestedLogCollectionStarts() {
            return List.copyOf(requestedLogCollectionStarts);
        }

        @Override
        protected ChangeEventSourceContext getContext() {
            return context;
        }

        @Override
        public OracleOffsetContext getOffsetContext() {
            return offsetContext;
        }

        @Override
        protected OraclePartition getPartition() {
            return partition;
        }

        @Override
        protected Optional<BoundedLogMiningWindow> prepareBoundedLogMiningWindow(Scn upperBoundsStartScn, Scn logCollectionStartScn)
                throws SQLException {
            if (windows.isEmpty()) {
                return Optional.empty();
            }

            requestedLogCollectionStarts.add(logCollectionStartScn);

            final WindowInput window = windows.remove();
            try {
                final Constructor<BoundedLogMiningWindow> constructor = BoundedLogMiningWindow.class
                        .getDeclaredConstructor(Scn.class, Scn.class, List.class);
                constructor.setAccessible(true);
                return Optional.of(constructor.newInstance(window.upperBoundScn, window.finalUpperBoundScn, window.logFiles));
            }
            catch (ReflectiveOperationException e) {
                throw new SQLException("Failed to create bounded mining window for test", e);
            }
        }

        @Override
        protected void pauseBetweenMiningSessions() {
        }

        @Override
        protected void captureJdbcSessionMemoryStatistics() {
        }

        @Override
        protected boolean waitForRangeAvailabilityInArchiveLogs(Scn startScn, Scn endScn) {
            return false;
        }

        @Override
        protected void executeBlockingSnapshot() {
        }

        private static class WindowInput {
            private final Scn upperBoundScn;
            private final Scn finalUpperBoundScn;
            private final List<LogFile> logFiles;

            private WindowInput(Scn upperBoundScn, Scn finalUpperBoundScn, List<LogFile> logFiles) {
                this.upperBoundScn = upperBoundScn;
                this.finalUpperBoundScn = finalUpperBoundScn;
                this.logFiles = logFiles;
            }
        }
    }
}
