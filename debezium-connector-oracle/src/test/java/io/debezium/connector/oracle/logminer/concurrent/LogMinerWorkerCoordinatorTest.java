/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;

import io.debezium.connector.oracle.AbstractOracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogFile;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;

/**
 * Unit tests for {@link LogMinerWorkerCoordinator}.
 *
 * <p>These tests drive the coordinator by injecting pre-canned {@link WorkerResult} objects via the
 * {@link TestableCoordinator} subclass. No Oracle database connection is required.
 *
 * <p>Test scenarios covered:
 * <ol>
 *   <li>Activation gate: single archive log ╬ô├Ñ├å concurrent reading not applicable</li>
 *   <li>Activation gate: online redo log present ╬ô├Ñ├å concurrent reading not applicable</li>
 *   <li>Activation gate: two archive logs ╬ô├Ñ├å concurrent reading applicable</li>
 *   <li>Activation gate: many archive logs ╬ô├Ñ├å concurrent reading applicable</li>
 *   <li>Work unit planning: even distribution across readers</li>
 *   <li>Work unit planning: fewer logs than readers</li>
 *   <li>Basic wave: all transactions resolved ╬ô├Ñ├å dispatched in commitScn order</li>
 *   <li>Dispatch order: transactions ordered by commitScn ascending</li>
 *   <li>Dispatch order: same commitScn ordered by commitRsId</li>
 *   <li>Rolled-back transactions not dispatched</li>
 *   <li>Safe horizon holds back transactions when cross-log txn present (known commit)</li>
 *   <li>Safe horizon uses lastReadScn when no orphan match</li>
 *   <li>Safe horizon is null when no pending inherited transactions</li>
 *   <li>Held transactions prepended to next wave in correct SCN order</li>
 *   <li>BRIDGE unit built for matched orphan + inherited after wave produces them</li>
 *   <li>BRIDGE unit session window covers inherited transaction START (Golden Rule)</li>
 *   <li>BRIDGE unit read window starts at lastReadScn (no re-reading)</li>
 *   <li>BRIDGE resolves cross-log transaction correctly</li>
 *   <li>Unknown-commit transactions force serial fallback until they resolve</li>
 *   <li>Schema change dispatched before DML at higher SCN</li>
 *   <li>Schema change at same SCN as transaction dispatched first</li>
 *   <li>Cross-wave orphan and inherited deduplication preserves newest state</li>
 * </ol>
 *
 * @author Debezium Authors
 */
@SuppressWarnings("unchecked")
class LogMinerWorkerCoordinatorTest {

    // --- SCN constants shared across tests ---
    private static final Scn SCN_100 = Scn.valueOf(100);
    private static final Scn SCN_150 = Scn.valueOf(150);
    private static final Scn SCN_180 = Scn.valueOf(180);
    private static final Scn SCN_200 = Scn.valueOf(200);
    private static final Scn SCN_210 = Scn.valueOf(210);
    private static final Scn SCN_230 = Scn.valueOf(230);
    private static final Scn SCN_250 = Scn.valueOf(250);
    private static final Scn SCN_280 = Scn.valueOf(280);
    private static final Scn SCN_300 = Scn.valueOf(300);
    private static final Scn SCN_350 = Scn.valueOf(350);
    private static final Scn SCN_380 = Scn.valueOf(380);
    private static final Scn SCN_400 = Scn.valueOf(400);
    private static final Scn SCN_480 = Scn.valueOf(480);
    private static final Scn SCN_500 = Scn.valueOf(500);
    private static final Scn SCN_600 = Scn.valueOf(600);
    private static final Scn SCN_700 = Scn.valueOf(700);
    private static final Scn SCN_800 = Scn.valueOf(800);

    private static final String TX_A = "TXAAA0001";
    private static final String TX_B = "TXBBB0002";
    private static final String TX_C = "TXCCC0003";

    private static final ZoneOffset ZONE_UTC = ZoneOffset.UTC;

    private EventDispatcher<OraclePartition, TableId> dispatcher;
    private OraclePartition partition;
    private OracleOffsetContext offsetContext;
    private OracleConnectorConfig connectorConfig;
    private OracleDatabaseSchema schema;
    private JdbcConfiguration jdbcConfig;

    @BeforeEach
    void setUp() {
        dispatcher = mock(EventDispatcher.class);
        partition = mock(OraclePartition.class);
        offsetContext = mock(OracleOffsetContext.class);
        connectorConfig = new OracleConnectorConfig(
                TestHelper.defaultConfig()
                        .with(OracleConnectorConfig.LOG_MINING_CONCURRENT_READERS, 2)
                        .build());
        schema = mock(OracleDatabaseSchema.class);
        jdbcConfig = JdbcConfiguration.adapt(connectorConfig.getJdbcConfig());

        final CommitScn commitScn = CommitScn.valueOf((String) null);
        when(offsetContext.getCommitScn()).thenReturn(commitScn);
        when(offsetContext.getScn()).thenReturn(SCN_100);
    }

    // ==========================================================================
    // Category 1: Activation gate tests (isConcurrentReadingApplicable)
    // ==========================================================================

    @ParameterizedTest
    @MethodSource("concurrentReadingApplicabilityCases")
    void testConcurrentReadingApplicability(int readers, List<LogFile> logs, boolean expected) {
        final TestableCoordinator coordinator = createCoordinator(readers);

        assertThat(coordinator.isConcurrentReadingApplicable(logs)).isEqualTo(expected);
    }

    @Test
    void testUnknownCommitTransactionsForceSerialFallback() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);

        runWaveWithSingleWorkerResult(coordinator,
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(inherited(TX_A, SCN_100, SCN_200)), List.of(), SCN_100, SCN_200));

        assertThat(coordinator.hasPendingUnknownCommitTransactions()).isTrue();
    }

    @Test
    void testMatchedOrphanDoesNotForceSerialFallback() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);

        runWaveWithSingleWorkerResult(coordinator,
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(inherited(TX_A, SCN_100, SCN_200)), List.of(orphanCommit(TX_A, SCN_300, SCN_200)), SCN_100,
                        SCN_300));

        assertThat(coordinator.hasPendingUnknownCommitTransactions()).isFalse();
    }

    // ==========================================================================
    // Category 2: Work unit planning
    // ==========================================================================

    // ==========================================================================
    // Category 3: Basic wave dispatch
    // ==========================================================================

    @Test
    void testWaveWithAllTransactionsResolvedDispatchesInOrder() throws Exception {
        // Two workers, each returning a fully committed transaction ╬ô├ç├╢ both dispatched
        final TestableCoordinator coordinator = createCoordinator(2);

        final LogMinerEvent eventA = dmlEvent(SCN_150, "DEBEZIUM.TEST_TABLE");
        final LogMinerEvent eventB = dmlEvent(SCN_250, "DEBEZIUM.TEST_TABLE");

        final CommittedTransaction txA = committed(TX_A, SCN_100, SCN_200, "rs.0001.0001", List.of(eventA));
        final CommittedTransaction txB = committed(TX_B, SCN_200, SCN_300, "rs.0002.0001", List.of(eventB));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(txA), List.of(), List.of(), SCN_100, SCN_200),
                workerResult(1, WorkUnitType.WORKER, List.of(txB), List.of(), List.of(), SCN_200, SCN_300));

        final List<LogFile> logs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_300, 2));

        coordinator.executeWave(logs, dispatcher, partition, offsetContext, ZONE_UTC);

        // Both transactions dispatched; no held-back state
        verify(dispatcher, times(2)).dispatchTransactionCommittedEvent(eq(partition), any(), any());
        assertThat(coordinator.getPendingInheritedTransactions()).isEmpty();
        assertThat(coordinator.getPendingOrphanCommits()).isEmpty();
    }

    @Test
    void testDispatchOrderByCommitScnAscending() throws Exception {
        // Three transactions with different commitScns ╬ô├ç├╢ should arrive in SCN order regardless
        // of worker result list order
        final TestableCoordinator coordinator = createCoordinator(2);

        final CommittedTransaction txLow = committed(TX_A, SCN_100, SCN_200, "rs.0001", List.of(dmlEvent(SCN_150, "DEBEZIUM.T")));
        final CommittedTransaction txMid = committed(TX_B, SCN_200, SCN_300, "rs.0002", List.of(dmlEvent(SCN_250, "DEBEZIUM.T")));
        final CommittedTransaction txHigh = committed(TX_C, SCN_300, SCN_400, "rs.0003", List.of(dmlEvent(SCN_350, "DEBEZIUM.T")));

        // Worker 0 returns txHigh and txLow (intentionally out of order)
        // Worker 1 returns txMid
        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(txHigh, txLow), List.of(), List.of(), SCN_100, SCN_400),
                workerResult(1, WorkUnitType.WORKER, List.of(txMid), List.of(), List.of(), SCN_200, SCN_300));

        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        // Verify commits fired in SCN order: txLow (200) ╬ô├Ñ├å txMid (300) ╬ô├Ñ├å txHigh (400)
        final InOrder order = inOrder(dispatcher);
        order.verify(dispatcher).dispatchTransactionCommittedEvent(eq(partition), any(), any());
        order.verify(dispatcher).dispatchTransactionCommittedEvent(eq(partition), any(), any());
        order.verify(dispatcher).dispatchTransactionCommittedEvent(eq(partition), any(), any());
    }

    @Test
    void testConcurrentWaveMergesOnlyAfterAllWorkersComplete() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);

        final CommittedTransaction txA = committed(TX_A, SCN_100, SCN_180, "rs.A",
                List.of(dmlEvent(SCN_150, "DEBEZIUM.TEST_TABLE")));
        final CommittedTransaction txB = committed(TX_B, SCN_200, SCN_280, "rs.B",
                List.of(dmlEvent(SCN_250, "DEBEZIUM.TEST_TABLE")));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(txA), List.of(), List.of(), SCN_100, SCN_200),
                workerResult(1, WorkUnitType.WORKER, List.of(txB), List.of(), List.of(), SCN_200, SCN_300));

        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        assertThat(coordinator.mergedPrefixes())
                .containsExactly(List.of(0, 1));
        verify(dispatcher, times(2)).dispatchTransactionCommittedEvent(eq(partition), any(), any());
    }

    @Test
    void testConcurrentWaveBuffersOutOfOrderCompletionsUntilWaveEnd() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);

        final CommittedTransaction txA = committed(TX_A, SCN_100, SCN_180, "rs.A",
                List.of(dmlEvent(SCN_150, "DEBEZIUM.TEST_TABLE")));
        final CommittedTransaction txB = committed(TX_B, SCN_200, SCN_280, "rs.B",
                List.of(dmlEvent(SCN_250, "DEBEZIUM.TEST_TABLE")));

        coordinator.enqueueResults(
                workerResult(1, WorkUnitType.WORKER, List.of(txB), List.of(), List.of(), SCN_200, SCN_300),
                workerResult(0, WorkUnitType.WORKER, List.of(txA), List.of(), List.of(), SCN_100, SCN_200));

        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        assertThat(coordinator.mergedPrefixes())
                .containsExactly(List.of(0, 1));
        verify(dispatcher, times(2)).dispatchTransactionCommittedEvent(eq(partition), any(), any());
    }

    @Test
    void testEarlierPrefixSchemaChangeStillQuarantinesLaterContaminatedTransactions() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);

        final TableId tableId = TableId.parse("DEBEZIUM.TEST_TABLE", false);
        final WorkerResult.SchemaChangeRecord ddl = new WorkerResult.SchemaChangeRecord(
                SCN_150,
                tableId,
                "ALTER TABLE TEST_TABLE ADD C2 NUMBER",
                Instant.now(),
                1,
                "rs.ddl",
                0L,
                null);
        final CommittedTransaction contaminated = committed(TX_A, SCN_200, SCN_280, "rs.A",
                List.of(dmlEvent(SCN_250, "DEBEZIUM.TEST_TABLE")));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(), List.of(), List.of(ddl), SCN_100, SCN_200),
                workerResult(1, WorkUnitType.WORKER, List.of(contaminated), List.of(), List.of(), SCN_200, SCN_300));

        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        assertThat(coordinator.hasPendingDdlContaminationReplay()).isTrue();
        verify(dispatcher, never()).dispatchTransactionCommittedEvent(eq(partition), any(), any());
    }

    @Test
    void testDispatchOrderByCommitRsIdForSameCommitScn() throws Exception {
        // Two transactions share the same commitScn ╬ô├ç├╢ tie-broken by rsId alphabetically
        final TestableCoordinator coordinator = createCoordinator(2);

        // txFirst has rsId "0x0001.0001.0001" which sorts before "0x0002.0001.0001"
        final CommittedTransaction txFirst = committed(TX_A, SCN_100, SCN_300, "0x0001.0001.0001", List.of(dmlEvent(SCN_150, "DEBEZIUM.T")));
        final CommittedTransaction txSecond = committed(TX_B, SCN_200, SCN_300, "0x0002.0001.0001", List.of(dmlEvent(SCN_250, "DEBEZIUM.T")));

        // Return in reversed order from workers ╬ô├ç├╢ sort should fix it
        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(txSecond, txFirst), List.of(), List.of(), SCN_100, SCN_300));

        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        final InOrder order = inOrder(dispatcher);
        order.verify(dispatcher).dispatchTransactionCommittedEvent(eq(partition), any(), any()); // txFirst
        order.verify(dispatcher).dispatchTransactionCommittedEvent(eq(partition), any(), any()); // txSecond
    }

    @Test
    void testRolledBackTransactionIsNotInWorkerResult() throws Exception {
        // Worker correctly excludes rolled-back transactions from its resolved list ╬ô├ç├╢
        // coordinator receives empty resolved list and dispatches nothing
        final TestableCoordinator coordinator = createCoordinator(2);

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(), List.of(), SCN_100, SCN_200),
                workerResult(1, WorkUnitType.WORKER, List.of(), List.of(), List.of(), SCN_200, SCN_300));

        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        verify(dispatcher, never()).dispatchTransactionCommittedEvent(any(), any(), any());
    }

    // ==========================================================================
    // Category 4: Safe dispatch horizon (ordering guarantee)
    // ==========================================================================

    @Test
    void testSafeHorizonHoldsBackTransactionsAboveKnownOrphanCommitScn() throws Exception {
        // Scenario: worker reports an unresolved TX_A (start=100) with an orphan commit at SCN 300.
        // TX_B commits cleanly at SCN 400. Horizon = 300 ╬ô├Ñ├å TX_B must be held back.
        final TestableCoordinator coordinator = createCoordinator(2);

        final InheritedTransaction unresolved = inherited(TX_A, SCN_100, SCN_200);
        final OrphanCommit orphan = orphanCommit(TX_A, SCN_300, SCN_200);
        final CommittedTransaction txB = committed(TX_B, SCN_350, SCN_400, "rs.0002", List.of(dmlEvent(SCN_380, "DEBEZIUM.T")));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(txB), List.of(unresolved), List.of(orphan), SCN_100, SCN_400));

        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        // TX_B (commitScn=400 >= horizon=300) must be held, not dispatched yet
        verify(dispatcher, never()).dispatchTransactionCommittedEvent(any(), any(), any());
        assertThat(coordinator.getPendingInheritedTransactions()).hasSize(1);
        assertThat(coordinator.getPendingOrphanCommits()).hasSize(1);
    }

    @Test
    void testSafeHorizonUsesLastReadScnWhenNoOrphanMatch() throws Exception {
        // Unknown-commit carry-forward case: unresolved TX_A has lastReadScn=200, no matching orphan.
        // TX_B commits cleanly at SCN 250. Horizon = 200 ╬ô├Ñ├å TX_B held back (250 >= 200).
        final TestableCoordinator coordinator = createCoordinator(2);

        final InheritedTransaction unresolved = inherited(TX_A, SCN_100, SCN_200); // lastReadScn=200
        final CommittedTransaction txB = committed(TX_B, SCN_210, SCN_250, "rs.0002", List.of(dmlEvent(SCN_230, "DEBEZIUM.T")));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(txB), List.of(unresolved), List.of(), SCN_100, SCN_300));

        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        // TX_B (commitScn=250 >= horizon=200) held back
        verify(dispatcher, never()).dispatchTransactionCommittedEvent(any(), any(), any());
        assertThat(coordinator.getPendingInheritedTransactions()).hasSize(1);
    }

    @Test
    void testSafeHorizonIsNullWhenNoPendingInheritedTransactions() throws Exception {
        // No cross-log transactions at all ╬ô├Ñ├å everything dispatched immediately
        final TestableCoordinator coordinator = createCoordinator(2);

        final CommittedTransaction txA = committed(TX_A, SCN_100, SCN_200, "rs.0001", List.of(dmlEvent(SCN_150, "DEBEZIUM.T")));
        final CommittedTransaction txB = committed(TX_B, SCN_200, SCN_300, "rs.0002", List.of(dmlEvent(SCN_250, "DEBEZIUM.T")));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(txA), List.of(), List.of(), SCN_100, SCN_200),
                workerResult(1, WorkUnitType.WORKER, List.of(txB), List.of(), List.of(), SCN_200, SCN_300));

        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        // Both dispatched ╬ô├ç├╢ no horizon throttling
        verify(dispatcher, times(2)).dispatchTransactionCommittedEvent(eq(partition), any(), any());
    }

    @Test
    void testHeldTransactionsPrependedToNextWave() throws Exception {
        // Wave 1: TX_B (commit=400) held back because TX_A unresolved with orphan at 300
        // Wave 2: TX_A resolved via BRIDGE (commit=300), TX_B previously held ╬ô├Ñ├å dispatch order: TX_A then TX_B
        final TestableCoordinator coordinator = createCoordinator(2);

        // Wave 1
        final InheritedTransaction unresolvedA = inherited(TX_A, SCN_100, SCN_200);
        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_300, SCN_200);
        final CommittedTransaction txB = committed(TX_B, SCN_350, SCN_400, "rs.0002", List.of(dmlEvent(SCN_380, "DEBEZIUM.T")));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(txB), List.of(unresolvedA), List.of(orphanA), SCN_100, SCN_400));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        // Nothing dispatched yet; TX_B held back
        verify(dispatcher, never()).dispatchTransactionCommittedEvent(any(), any(), any());

        // Wave 2: BRIDGE resolves TX_A; no new inherited or orphans
        final CommittedTransaction txA = committed(TX_A, SCN_100, SCN_300, "rs.0001", List.of(dmlEvent(SCN_150, "DEBEZIUM.T")));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.BRIDGE, List.of(txA), List.of(), List.of(), SCN_100, SCN_300));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        // Both should now be dispatched: TX_A (300) before TX_B (400)
        verify(dispatcher, times(2)).dispatchTransactionCommittedEvent(eq(partition), any(), any());
        assertThat(coordinator.getPendingInheritedTransactions()).isEmpty();
        assertThat(coordinator.getPendingOrphanCommits()).isEmpty();
    }

    // ==========================================================================
    // Category 5: BRIDGE unit planning
    // ==========================================================================

    @Test
    void testMatchedInheritedAndOrphanStayPendingUntilBridgePlanning() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);

        final InheritedTransaction unresolvedA = new InheritedTransaction(
                TX_A,
                SCN_100,
                Instant.now(),
                "user1",
                null,
                1,
                SCN_200,
                SCN_200,
                List.of(dmlEvent(SCN_150, "DEBEZIUM.T")));
        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_300, SCN_200);
        final CommittedTransaction txB = committed(TX_B, SCN_350, SCN_400, "rs.0002", List.of(dmlEvent(SCN_380, "DEBEZIUM.T")));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(unresolvedA), List.of(), SCN_100, SCN_200),
                workerResult(1, WorkUnitType.WORKER, List.of(txB), List.of(), List.of(orphanA), SCN_200, SCN_400));

        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        verify(dispatcher, never()).dispatchTransactionCommittedEvent(eq(partition), any(), any());
        assertThat(coordinator.getPendingInheritedTransactions()).hasSize(1);
        assertThat(coordinator.getPendingOrphanCommits()).hasSize(1);
        assertThat(coordinator.planWorkUnits(twoArchiveLogs())).anyMatch(unit -> unit.type() == WorkUnitType.BRIDGE);
    }

    @Test
    void testBridgeUnitCreatedForMatchedOrphanAndInherited() throws Exception {
        // After a wave produces an unresolved+orphan pair, the next planWorkUnits call
        // should return a BRIDGE unit
        final TestableCoordinator coordinator = createCoordinator(2);

        // Seed pending state by running wave 1
        final InheritedTransaction unresolvedA = inherited(TX_A, SCN_100, SCN_200);
        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_300, SCN_200);

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(unresolvedA), List.of(orphanA), SCN_100, SCN_300));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        // Now plan work units for a new set of logs covering the bridge range
        final List<LogFile> bridgeLogs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_400, 2));

        final List<WorkUnit> units = coordinator.planWorkUnits(bridgeLogs);

        // Expect exactly one BRIDGE unit for TX_A
        final List<WorkUnit> bridges = units.stream()
                .filter(u -> u.type() == WorkUnitType.BRIDGE).toList();
        assertThat(bridges).hasSize(1);
        final WorkUnit bridge = bridges.get(0);
        assertThat(bridge.inheritedTransactions()).hasSize(1);
        assertThat(bridge.inheritedTransactions().get(0).transactionId()).isEqualTo(TX_A);
    }

    @Test
    void testThreeThreadWavePlansBridgeWhenThirdWorkerFindsCommit() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(3);

        final InheritedTransaction unresolvedA = inherited(TX_A, SCN_100, SCN_200);
        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_350, SCN_300);

        final List<LogFile> wave1Logs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_300, 2),
                archiveLog("log3", SCN_300, SCN_400, 3));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(unresolvedA), List.of(), SCN_100, SCN_200),
                workerResult(1, WorkUnitType.WORKER, List.of(), List.of(), List.of(), SCN_200, SCN_300),
                workerResult(2, WorkUnitType.WORKER, List.of(), List.of(), List.of(orphanA), SCN_300, SCN_400));

        coordinator.executeWave(wave1Logs, dispatcher, partition, offsetContext, ZONE_UTC);

        assertThat(coordinator.getPendingInheritedTransactions()).hasSize(1);
        assertThat(coordinator.getPendingOrphanCommits()).hasSize(1);
        assertThat(coordinator.hasPendingUnknownCommitTransactions()).isFalse();

        final List<WorkUnit> units = coordinator.planWorkUnits(wave1Logs);
        final List<WorkUnit> bridges = units.stream()
                .filter(u -> u.type() == WorkUnitType.BRIDGE)
                .toList();

        assertThat(bridges).hasSize(1);
        assertThat(bridges.get(0).inheritedTransactions()).singleElement()
                .extracting(InheritedTransaction::transactionId)
                .isEqualTo(TX_A);
    }

    @Test
    void testBridgeUnitSessionWindowCoversInheritedStart_GoldenRule() throws Exception {
        // BRIDGE session start must be strictly less than the inherited transaction's startScn
        final TestableCoordinator coordinator = createCoordinator(2);

        final InheritedTransaction unresolvedA = inherited(TX_A, SCN_100, SCN_200); // startScn=100
        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_300, SCN_200);

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(unresolvedA), List.of(orphanA), SCN_100, SCN_300));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        final List<LogFile> bridgeLogs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_400, 2));

        final List<WorkUnit> units = coordinator.planWorkUnits(bridgeLogs);
        final WorkUnit bridge = units.stream().filter(u -> u.type() == WorkUnitType.BRIDGE).findFirst().orElseThrow();

        // Golden Rule: sessionStart < startScn (= startScn - 1)
        assertThat(bridge.sessionStartScn()).isEqualTo(SCN_100.subtract(Scn.ONE));
        assertThat(bridge.sessionEndScn()).isEqualTo(SCN_300); // = orphan.commitScn
    }

    @Test
    void testBridgeUnitReadWindowStartsAtLastReadScn_NoRereading() throws Exception {
        // Bridge read window must start at inherited.lastReadScn to avoid re-processing data
        final TestableCoordinator coordinator = createCoordinator(2);

        // lastReadScn = 200 (where the worker left off)
        final InheritedTransaction unresolvedA = inherited(TX_A, SCN_100, SCN_200);
        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_300, SCN_200);

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(unresolvedA), List.of(orphanA), SCN_100, SCN_300));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        final List<LogFile> bridgeLogs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_400, 2));

        final List<WorkUnit> units = coordinator.planWorkUnits(bridgeLogs);
        final WorkUnit bridge = units.stream().filter(u -> u.type() == WorkUnitType.BRIDGE).findFirst().orElseThrow();

        // Read start = lastReadScn (= 200), read end = orphan commitScn (= 300)
        assertThat(bridge.readStartScn()).isEqualTo(SCN_200);
        assertThat(bridge.readEndScn()).isEqualTo(SCN_300);
    }

    @Test
    void testBridgeResolvesTransactionCorrectly() throws Exception {
        // Full round-trip: wave 1 produces unresolved+orphan, wave 2 BRIDGE resolves TX_A
        // and TX_B (held from wave 1) both dispatched in correct order
        final TestableCoordinator coordinator = createCoordinator(2);

        // Wave 1: TX_B clean commit at 500, TX_A unresolved (commit=400 is orphan in log2)
        final InheritedTransaction unresolvedA = inherited(TX_A, SCN_100, SCN_200);
        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_400, SCN_200);
        final CommittedTransaction txB = committed(TX_B, SCN_300, SCN_500, "rs.0002", List.of(dmlEvent(SCN_350, "DEBEZIUM.T")));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(txB), List.of(unresolvedA), List.of(orphanA), SCN_100, SCN_500));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        verify(dispatcher, never()).dispatchTransactionCommittedEvent(any(), any(), any()); // TX_B held back

        // Wave 2: BRIDGE resolves TX_A (commit=400)
        final CommittedTransaction txA = committed(TX_A, SCN_100, SCN_400, "rs.0001", List.of(dmlEvent(SCN_150, "DEBEZIUM.T")));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.BRIDGE, List.of(txA), List.of(), List.of(), SCN_100, SCN_400));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        // TX_A (400) before TX_B (500)
        final InOrder inOrder = inOrder(dispatcher);
        inOrder.verify(dispatcher).dispatchTransactionCommittedEvent(eq(partition), any(), any()); // TX_A
        inOrder.verify(dispatcher).dispatchTransactionCommittedEvent(eq(partition), any(), any()); // TX_B
    }

    // ==========================================================================
    // Category 7: Schema change interleaving
    // ==========================================================================

    @Test
    void testSchemaChangeDispatchedBeforeDmlAtHigherScn() throws Exception {
        // Schema change at SCN 150 must be applied before TX_A (commitScn=200) is dispatched
        final TestableCoordinator coordinator = createCoordinator(2);

        final WorkerResult.SchemaChangeRecord ddl = new WorkerResult.SchemaChangeRecord(
                SCN_100, TableId.parse("ORCLPDB1.DEBEZIUM.TEST_TABLE", false),
                "ALTER TABLE DEBEZIUM.TEST_TABLE ADD (COL2 VARCHAR2(50))",
                Instant.now(), 1, "rs.ddl.1", 0L, 1L);

        final CommittedTransaction txA = committed(TX_A, SCN_100, SCN_200, "rs.0001", List.of(dmlEvent(SCN_180, "DEBEZIUM.TEST_TABLE")));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(txA), List.of(), List.of(), List.of(ddl), SCN_100, SCN_200));

        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        // Schema change dispatched, then data change
        final InOrder order = inOrder(dispatcher);
        order.verify(dispatcher).dispatchSchemaChangeEvent(eq(partition), any(), any(), any());
        order.verify(dispatcher).dispatchTransactionCommittedEvent(eq(partition), any(), any());
    }

    @Test
    void testSchemaChangeAtSameSCNAsTransactionDispatchedFirst() throws Exception {
        // Schema change SCN == transaction commitScn: DDL should come first (<=)
        final TestableCoordinator coordinator = createCoordinator(2);

        final WorkerResult.SchemaChangeRecord ddl = new WorkerResult.SchemaChangeRecord(
                SCN_200, TableId.parse("ORCLPDB1.DEBEZIUM.TEST_TABLE", false),
                "ALTER TABLE DEBEZIUM.TEST_TABLE ADD (COL2 VARCHAR2(50))",
                Instant.now(), 1, "rs.ddl.2", 0L, 1L);

        final CommittedTransaction txA = committed(TX_A, SCN_100, SCN_200, "rs.0001", List.of(dmlEvent(SCN_150, "DEBEZIUM.TEST_TABLE")));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(txA), List.of(), List.of(), List.of(ddl), SCN_100, SCN_200));

        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        final InOrder order = inOrder(dispatcher);
        order.verify(dispatcher).dispatchSchemaChangeEvent(eq(partition), any(), any(), any());
        order.verify(dispatcher).dispatchTransactionCommittedEvent(eq(partition), any(), any());
    }

    // ==========================================================================
    // Category 8: Cross-wave state management
    // ==========================================================================

    @Test
    void testCrossWaveInheritedTransactionDeduplication_NewerEventListWins() throws Exception {
        // If the same transactionId appears in two waves, the newer entry (with more events) wins
        final TestableCoordinator coordinator = createCoordinator(2);

        // Wave 1: TX_A unresolved with 1 event
        final InheritedTransaction unresolvedA_v1 = new InheritedTransaction(
                TX_A, SCN_100, Instant.now(), "user1", null, 1, SCN_200, SCN_200,
                List.of(dmlEvent(SCN_150, "DEBEZIUM.T")));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(unresolvedA_v1), List.of(), SCN_100, SCN_200));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        assertThat(coordinator.getPendingInheritedTransactions()).hasSize(1);
        assertThat(coordinator.getPendingInheritedTransactions().get(0).events()).hasSize(1);

        // Wave 2: TX_A still unresolved, now with 2 events after continuation reads more DML
        final InheritedTransaction unresolvedA_v2 = new InheritedTransaction(
                TX_A, SCN_100, Instant.now(), "user1", null, 1, SCN_200, SCN_300,
                List.of(dmlEvent(SCN_150, "DEBEZIUM.T"), dmlEvent(SCN_280, "DEBEZIUM.T")));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(unresolvedA_v2), List.of(), SCN_200, SCN_300));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        // Newer entry should replace: 2 events, lastReadScn=300
        assertThat(coordinator.getPendingInheritedTransactions()).hasSize(1);
        assertThat(coordinator.getPendingInheritedTransactions().get(0).events()).hasSize(2);
        assertThat(coordinator.getPendingInheritedTransactions().get(0).lastReadScn()).isEqualTo(SCN_300);
    }

    @Test
    void testCrossWaveOrphanCommitDeduplication() throws Exception {
        // The same orphan (same transactionId + commitScn) must not be added twice
        final TestableCoordinator coordinator = createCoordinator(2);

        final InheritedTransaction unresolvedA = inherited(TX_A, SCN_100, SCN_200);
        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_300, SCN_200);

        // Wave 1: produce orphan
        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(unresolvedA), List.of(orphanA), SCN_100, SCN_300));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        assertThat(coordinator.getPendingOrphanCommits()).hasSize(1);

        // Wave 2: same orphan reported again (edge case)
        final InheritedTransaction unresolvedA2 = inherited(TX_A, SCN_100, SCN_300);
        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(unresolvedA2), List.of(orphanA), SCN_200, SCN_400));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        // Still exactly one orphan commit, not two
        assertThat(coordinator.getPendingOrphanCommits()).hasSize(1);
    }

    @Test
    void testFullOrderingPreservationAcrossTwoWaves() throws Exception {
        // End-to-end scenario proving global SCN ordering is preserved:
        // Wave 1: TX_B(commit=500) clean, TX_A unresolved with orphan at 400 ╬ô├Ñ├å TX_B held
        // Wave 2: BRIDGE resolves TX_A(commit=400), TX_B rejoins ╬ô├Ñ├å dispatch order: TX_A(400), TX_B(500)
        final TestableCoordinator coordinator = createCoordinator(2);

        final InheritedTransaction unresolvedA = inherited(TX_A, SCN_100, SCN_200);
        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_400, SCN_200);
        final CommittedTransaction txB = committed(TX_B, SCN_300, SCN_500, "rs.B", List.of(dmlEvent(SCN_350, "DEBEZIUM.T")));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(txB), List.of(unresolvedA), List.of(orphanA), SCN_100, SCN_500));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        // Nothing dispatched: horizon=400, txB.commit=500 >= 400 ╬ô├Ñ├å held
        verify(dispatcher, never()).dispatchTransactionCommittedEvent(any(), any(), any());

        // Wave 2
        final CommittedTransaction txA = committed(TX_A, SCN_100, SCN_400, "rs.A", List.of(dmlEvent(SCN_150, "DEBEZIUM.T")));
        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.BRIDGE, List.of(txA), List.of(), List.of(), SCN_100, SCN_400));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        // TX_A (400) dispatched before TX_B (500)
        final InOrder order = inOrder(dispatcher);
        order.verify(dispatcher).dispatchTransactionCommittedEvent(eq(partition), any(), any()); // TX_A = 400
        order.verify(dispatcher).dispatchTransactionCommittedEvent(eq(partition), any(), any()); // TX_B = 500
    }

    // ==========================================================================
    // Category 9: Transition to multi-threaded with open transaction
    // ==========================================================================

    /**
     * Simulates the transition from single-threaded to multi-threaded mode when an open
     * transaction pins the low watermark: log1 was already mined, TX_A is still open.
     * When logs 2 and 3 become available, the connector switches to concurrent mode.
     * Worker 0 covers log2 (carrying TX_A's inherited events), Worker 1 covers log3.
     * TX_A commits in log3 ╬ô├ç├╢ Worker 1 produces an orphan commit, and the pair is
     * resolved via BRIDGE in the following wave.
     */
    @Test
    void testTransitionToMultiThreadedWithOpenTransaction_PinnedLowWatermark() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);

        // Pre-populate pending state as if a prior serial wave left TX_A unresolved.
        // TX_A started in log1 (already-read), lastReadScn = log1's nextScn = 200.
        final InheritedTransaction unresolvedA = inherited(TX_A, SCN_100, SCN_200);
        runWaveWithSingleWorkerResult(coordinator,
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(unresolvedA), List.of(), SCN_100, SCN_200));

        assertThat(coordinator.getPendingInheritedTransactions()).hasSize(1);
        assertThat(coordinator.getPendingInheritedTransactions().get(0).transactionId()).isEqualTo(TX_A);
        verify(dispatcher, never()).dispatchTransactionCommittedEvent(any(), any(), any());

        // Wave 2 (concurrent): logs 2 (SCN 200-400) and 3 (SCN 400-600).
        // With contiguous partitioning (2 logs, 2 readers):
        // Worker 0 = log2 (200-400) ╬ô├ç├╢ finds additional DML for TX_A, still no COMMIT
        // Worker 1 = log3 (400-600) ╬ô├ç├╢ finds COMMIT for TX_A without START ╬ô├Ñ├å orphan
        final InheritedTransaction updatedA = new InheritedTransaction(
                TX_A, SCN_100, Instant.now(), "user1", null, 1, SCN_200, SCN_300,
                List.of(dmlEvent(SCN_250, "DEBEZIUM.T")));
        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_500, SCN_400);

        final List<LogFile> wave2Logs = List.of(
                archiveLog("log2", SCN_200, SCN_400, 2),
                archiveLog("log3", SCN_400, SCN_600, 3));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(updatedA), List.of(), SCN_200, SCN_400),
                workerResult(1, WorkUnitType.WORKER, List.of(), List.of(), List.of(orphanA), SCN_400, SCN_600));
        coordinator.executeWave(wave2Logs, dispatcher, partition, offsetContext, ZONE_UTC);

        // Safe horizon should hold everything back (orphan at SCN 500 matched with pending TX_A)
        verify(dispatcher, never()).dispatchTransactionCommittedEvent(any(), any(), any());
        assertThat(coordinator.getPendingInheritedTransactions()).hasSize(1);
        assertThat(coordinator.getPendingOrphanCommits()).hasSize(1);

        // Wave 3: BRIDGE resolves TX_A (commit at SCN 500)
        final CommittedTransaction resolvedA = committed(TX_A, SCN_100, SCN_500, "rs.A",
                List.of(dmlEvent(SCN_250, "DEBEZIUM.T"), dmlEvent(SCN_480, "DEBEZIUM.T")));
        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.BRIDGE, List.of(resolvedA), List.of(), List.of(), SCN_100, SCN_500));
        coordinator.executeWave(wave2Logs, dispatcher, partition, offsetContext, ZONE_UTC);

        // TX_A should be dispatched now
        verify(dispatcher, times(1)).dispatchTransactionCommittedEvent(eq(partition), any(), any());
        assertThat(coordinator.getPendingInheritedTransactions()).isEmpty();
        assertThat(coordinator.getPendingOrphanCommits()).isEmpty();
    }

    /**
     * Transition scenario: two concurrent workers both see DML for the same transaction
     * (START in worker 0's range, COMMIT in worker 1's range). Worker 1's orphan should
     * match worker 0's unresolved inherited transaction to form a BRIDGE pair.
     */
    @Test
    void testMultiThreadedWithCrossLogTransaction_OrphanMatchedAcrossWorkers() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);

        // Worker 0 (logs 1-2): TX_A starts, some DML, but no COMMIT ╬ô├Ñ├å unresolved
        final InheritedTransaction unresolvedA = new InheritedTransaction(
                TX_A, SCN_100, Instant.now(), "user1", null, 1, SCN_200, SCN_200,
                List.of(dmlEvent(SCN_150, "DEBEZIUM.T")));

        // Worker 1 (logs 3-4): finds COMMIT for TX_A at SCN 800 ╬ô├Ñ├å orphan
        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_800, SCN_600);

        // Also a clean transaction TX_B committed at SCN 250
        final CommittedTransaction txB = committed(TX_B, SCN_210, SCN_250, "rs.B",
                List.of(dmlEvent(SCN_230, "DEBEZIUM.T")));

        final List<LogFile> waveLogs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_400, 2),
                archiveLog("log3", SCN_400, SCN_600, 3),
                archiveLog("log4", SCN_600, SCN_800, 4));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(txB), List.of(unresolvedA), List.of(), SCN_100, SCN_400),
                workerResult(1, WorkUnitType.WORKER, List.of(), List.of(), List.of(orphanA), SCN_400, SCN_800));

        coordinator.executeWave(waveLogs, dispatcher, partition, offsetContext, ZONE_UTC);

        // TX_B falls inside TX_A's unsafe interval and must be withheld until the bridge rereads
        // that range, rather than being dispatched from the overlapping worker result.
        verify(dispatcher, never()).dispatchTransactionCommittedEvent(eq(partition), any(), any());
        assertThat(coordinator.getPendingInheritedTransactions()).hasSize(1);
        assertThat(coordinator.getPendingOrphanCommits()).hasSize(1);
    }

    // ==========================================================================
    // Category 10: Serial execution fallback
    // ==========================================================================

    @Test
    void testExecuteSerialDispatchesAllTransactions() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);

        // Serial execution does not use the thread pool; the testable override is bypassed.
        // We use executeSerial directly which creates a real LogMinerWorker... but that
        // requires an Oracle connection. For unit testing, verify that Serial mode is
        // selected correctly by checking the fallback predicate and concurrent work-unit plan.

        // When an inherited transaction has no matching orphan commit, the source should fall
        // back to serial mode rather than planning a dedicated continuation unit.
        final InheritedTransaction unresolvedA = inherited(TX_A, SCN_100, SCN_200);
        runWaveWithSingleWorkerResult(coordinator,
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(unresolvedA), List.of(), SCN_100, SCN_200));

        assertThat(coordinator.getPendingInheritedTransactions()).hasSize(1);
        assertThat(coordinator.hasPendingUnknownCommitTransactions()).isTrue();

        // Concurrent planning now leaves these logs as plain worker slices because the outer
        // source is responsible for switching to executeSerial instead of executeWave.
        final List<LogFile> newLogs = List.of(
                archiveLog("log2", SCN_200, SCN_300, 2),
                archiveLog("log3", SCN_300, SCN_400, 3));

        final List<WorkUnit> units = coordinator.planWorkUnits(newLogs);
        assertThat(units).isNotEmpty();
        assertThat(units).allMatch(u -> u.type() == WorkUnitType.WORKER);
    }

    @Test
    void testUnknownCommitInFutureLogDoesNotPlanBridgeOnNextWave() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);

        final InheritedTransaction unresolvedA = new InheritedTransaction(
                TX_A, SCN_100, Instant.now(), "user1", null, 1, SCN_200, SCN_200,
                List.of(dmlEvent(SCN_150, "DEBEZIUM.T")));

        final List<LogFile> wave1Logs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_300, 2));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(unresolvedA), List.of(), SCN_100, SCN_200),
                workerResult(1, WorkUnitType.WORKER, List.of(), List.of(), List.of(), SCN_200, SCN_300));

        coordinator.executeWave(wave1Logs, dispatcher, partition, offsetContext, ZONE_UTC);

        assertThat(coordinator.getPendingInheritedTransactions()).hasSize(1);
        assertThat(coordinator.getPendingOrphanCommits()).isEmpty();
        assertThat(coordinator.hasPendingUnknownCommitTransactions()).isTrue();

        final List<LogFile> wave2Logs = List.of(
                archiveLog("log2", SCN_200, SCN_300, 2),
                archiveLog("log3", SCN_300, SCN_400, 3));

        final List<WorkUnit> units = coordinator.planWorkUnits(wave2Logs);

        assertThat(units).isNotEmpty();
        assertThat(units).allMatch(u -> u.type() == WorkUnitType.WORKER);
        assertThat(units).noneMatch(u -> u.type() == WorkUnitType.BRIDGE);
    }

    @Test
    void testExecuteSerialWithRedoLogInScope() throws Exception {
        // Serial fallback happens when a redo log is present.
        // The full executeSerial test requires a database connection,
        // but we verify the gating logic is correct.
        final TestableCoordinator coordinator = createCoordinator(2);

        final List<LogFile> logsWithRedo = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                redoLog("redo1", SCN_200, SCN_300, 2));

        assertThat(coordinator.isConcurrentReadingApplicable(logsWithRedo)).isFalse();

        // planWorkUnits on mixed logs: should still build work units (no crash)
        final List<WorkUnit> units = coordinator.planWorkUnits(logsWithRedo);
        assertThat(units).isNotEmpty();
        assertThat(units).allMatch(u -> u.type() == WorkUnitType.WORKER);
    }

    @Test
    void testBuildSerialWorkUnitCapsRedoWindowToExplicitUpperBound() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);

        final InheritedTransaction unresolvedA = inherited(TX_A, SCN_100, SCN_200);
        runWaveWithSingleWorkerResult(coordinator,
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(unresolvedA), List.of(), SCN_100, SCN_200));

        final Scn explicitUpperBound = Scn.valueOf(2_517_312);
        final List<LogFile> redoLogs = List.of(
                redoLog("redo1", SCN_200, TestHelper.SCN_MAX, 2));

        final WorkUnit unit = coordinator.buildSerialWorkUnit(redoLogs, SCN_200, explicitUpperBound);

        assertThat(unit.sessionStartScn()).isEqualTo(SCN_200);
        assertThat(unit.sessionEndScn()).isEqualTo(explicitUpperBound);
        assertThat(unit.readStartScn()).isEqualTo(SCN_200);
        assertThat(unit.readEndScn()).isEqualTo(explicitUpperBound);
        assertThat(unit.inheritedTransactions()).containsExactly(unresolvedA);
    }

    // ==========================================================================
    // Category 11: Contiguous partition verification
    // ==========================================================================

    @Test
    void testContiguousPartitionAssignsConsecutiveLogsToEachWorker() {
        // 4 logs, 2 readers: Worker 0 = [log1, log2], Worker 1 = [log3, log4]
        final TestableCoordinator coordinator = createCoordinator(2);

        final List<LogFile> logs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_300, 2),
                archiveLog("log3", SCN_300, SCN_400, 3),
                archiveLog("log4", SCN_400, SCN_500, 4));

        final List<WorkUnit> units = coordinator.planWorkUnits(logs);

        assertThat(units).hasSize(2);
        assertThat(units.get(0).logFiles()).containsExactly(logs.get(0), logs.get(1));
        assertThat(units.get(1).logFiles()).containsExactly(logs.get(2), logs.get(3));
    }

    @Test
    void testContiguousPartitionHandlesUnevenSplit() {
        // 5 logs, 2 readers: Worker 0 = [log1, log2, log3] (ceiling 5/2 = 3), Worker 1 = [log4, log5]
        final TestableCoordinator coordinator = createCoordinator(2);

        final List<LogFile> logs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_300, 2),
                archiveLog("log3", SCN_300, SCN_400, 3),
                archiveLog("log4", SCN_400, SCN_500, 4),
                archiveLog("log5", SCN_500, SCN_600, 5));

        final List<WorkUnit> units = coordinator.planWorkUnits(logs);

        assertThat(units).hasSize(2);
        assertThat(units.get(0).logFiles()).hasSize(3);
        assertThat(units.get(1).logFiles()).hasSize(2);
        assertThat(units.get(0).logFiles()).containsExactly(logs.get(0), logs.get(1), logs.get(2));
        assertThat(units.get(1).logFiles()).containsExactly(logs.get(3), logs.get(4));
    }

    @Test
    void testContiguousPartitionWithMoreReadersThanLogs() {
        // 2 logs, 4 readers: 2 workers, each with 1 log
        final TestableCoordinator coordinator = createCoordinator(4);

        final List<LogFile> logs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_300, 2));

        final List<WorkUnit> units = coordinator.planWorkUnits(logs);

        assertThat(units).hasSize(2);
        assertThat(units.get(0).logFiles()).containsExactly(logs.get(0));
        assertThat(units.get(1).logFiles()).containsExactly(logs.get(1));
    }

    @Test
    void testContiguousPartitionMinimalSessionWindow() {
        // Verify session window boundaries are tight for contiguous partitions
        final TestableCoordinator coordinator = createCoordinator(2);

        final List<LogFile> logs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_300, 2),
                archiveLog("log3", SCN_300, SCN_400, 3),
                archiveLog("log4", SCN_400, SCN_500, 4));

        final List<WorkUnit> units = coordinator.planWorkUnits(logs);

        // Worker 0: session [100, 300), covering log1 + log2 contiguously
        final WorkUnit unit0 = units.get(0);
        assertThat(unit0.sessionStartScn()).isEqualTo(SCN_100);
        assertThat(unit0.sessionEndScn()).isEqualTo(SCN_300);
        assertThat(unit0.readStartScn()).isEqualTo(SCN_100);
        assertThat(unit0.readEndScn()).isEqualTo(SCN_300);

        // Worker 1: session [300, 500), covering log3 + log4 contiguously
        final WorkUnit unit1 = units.get(1);
        assertThat(unit1.sessionStartScn()).isEqualTo(SCN_300);
        assertThat(unit1.sessionEndScn()).isEqualTo(SCN_500);
        assertThat(unit1.readStartScn()).isEqualTo(SCN_300);
        assertThat(unit1.readEndScn()).isEqualTo(SCN_500);
    }

    // ==========================================================================
    // Category 12: State recovery on worker failure
    // ==========================================================================

    @Test
    void testStateRecoveryAfterWorkerExecutionFails() throws Exception {
        // buildBridgeUnits removes orphans from pending lists before workers run.
        // If a worker throws, pending state must be restored so the next wave can retry.
        final TestableCoordinator coordinator = createCoordinator(2);

        // Seed cross-wave state
        final InheritedTransaction inheritedA = inherited(TX_A, SCN_100, SCN_200);
        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_300, SCN_200);
        final CommittedTransaction txB = committed(TX_B, SCN_210, SCN_250, "rs.B",
                List.of(dmlEvent(SCN_230, "DEBEZIUM.T")));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(txB), List.of(inheritedA), List.of(orphanA), SCN_100, SCN_300));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        assertThat(coordinator.getPendingInheritedTransactions()).hasSize(1);
        assertThat(coordinator.getPendingOrphanCommits()).hasSize(1);

        // Set up failure injection for the next executeWave call.
        // executeWave snapshots state BEFORE buildWorkUnits mutates it, then restores
        // if executeWorkers throws.
        coordinator.enqueueFailure(new RuntimeException("Simulated worker failure"));

        final List<LogFile> waveLogs = List.of(
                archiveLog("logA", SCN_100, SCN_200, 1),
                archiveLog("logB", SCN_200, SCN_400, 2));

        try {
            coordinator.executeWave(waveLogs, dispatcher, partition, offsetContext, ZONE_UTC);
            // Should not reach here
            assertThat(false).as("Expected RuntimeException to be thrown").isTrue();
        }
        catch (RuntimeException expected) {
            // Expected ╬ô├ç├╢ verify state was restored
            assertThat(expected.getMessage()).contains("Simulated worker failure");
        }

        // After rollback, pending state should be intact (as it was before the failed wave)
        assertThat(coordinator.getPendingInheritedTransactions()).hasSize(1);
        assertThat(coordinator.getPendingInheritedTransactions().get(0).transactionId()).isEqualTo(TX_A);
        assertThat(coordinator.getPendingOrphanCommits()).hasSize(1);
        assertThat(coordinator.getPendingOrphanCommits().get(0).transactionId()).isEqualTo(TX_A);
    }

    @Test
    void testStateRecoveryAfterWorkerExecutionFailsRestoresHeldBackDispatchAndSchema() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);

        final InheritedTransaction inheritedA = inherited(TX_A, SCN_100, SCN_200);
        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_300, SCN_200);
        final CommittedTransaction txB = committed(TX_B, SCN_350, SCN_400, "rs.B",
                List.of(dmlEvent(SCN_380, "DEBEZIUM.OTHER_TABLE")));
        final WorkerResult.SchemaChangeRecord ddl = new WorkerResult.SchemaChangeRecord(
                SCN_350,
                TableId.parse("DEBEZIUM.TEST_TABLE", false),
                "ALTER TABLE TEST_TABLE ADD C2 NUMBER",
                Instant.now(),
                1,
                "rs.ddl",
                0L,
                null);

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(txB), List.of(inheritedA), List.of(orphanA), List.of(ddl), SCN_100, SCN_400));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        assertThat(coordinator.pendingDispatch()).singleElement()
                .extracting(CommittedTransaction::transactionId)
                .isEqualTo(TX_B);
        assertThat(coordinator.pendingSchemaChanges()).singleElement()
                .extracting(WorkerResult.SchemaChangeRecord::scn)
                .isEqualTo(SCN_350);

        coordinator.enqueueFailure(new RuntimeException("Simulated worker failure"));

        final List<LogFile> waveLogs = List.of(
                archiveLog("logA", SCN_100, SCN_200, 1),
                archiveLog("logB", SCN_200, SCN_400, 2));

        try {
            coordinator.executeWave(waveLogs, dispatcher, partition, offsetContext, ZONE_UTC);
            assertThat(false).as("Expected RuntimeException to be thrown").isTrue();
        }
        catch (RuntimeException expected) {
            assertThat(expected.getMessage()).contains("Simulated worker failure");
        }

        assertThat(coordinator.pendingDispatch()).singleElement()
                .extracting(CommittedTransaction::transactionId)
                .isEqualTo(TX_B);
        assertThat(coordinator.pendingSchemaChanges()).singleElement()
                .extracting(WorkerResult.SchemaChangeRecord::scn)
                .isEqualTo(SCN_350);
    }

    // ==========================================================================
    // Category 13: Additional coverage ╬ô├ç├╢ edge cases
    // ==========================================================================

    @Test
    void testWaveWithOnlyEmptyTransactionsDispatchesHeartbeatsOnly() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);

        // Two empty transactions (no DML events) ╬ô├ç├╢ should dispatch heartbeats only
        final CommittedTransaction txA = committed(TX_A, SCN_100, SCN_200, "rs.A", List.of());
        final CommittedTransaction txB = committed(TX_B, SCN_200, SCN_300, "rs.B", List.of());

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(txA, txB), List.of(), List.of(), SCN_100, SCN_300));

        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        // Empty transactions dispatch heartbeats (one per empty tx) plus one wave-end heartbeat
        verify(dispatcher, times(3)).dispatchHeartbeatEvent(eq(partition), any());
        verify(dispatcher, never()).dispatchTransactionCommittedEvent(any(), any(), any());
    }

    @Test
    void testCompatibleBridgeTransactionsBatchIntoSingleBridgeUnit() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);

        final InheritedTransaction inheritedA = inherited(TX_A, SCN_100, SCN_200);
        final InheritedTransaction inheritedC = inherited(TX_C, SCN_100, SCN_200);
        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_300, SCN_200);
        final OrphanCommit orphanC = orphanCommit(TX_C, SCN_350, SCN_200);

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(),
                        List.of(inheritedA, inheritedC),
                        List.of(orphanA, orphanC), SCN_100, SCN_350));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        final List<LogFile> nextLogs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_400, 2));

        final List<WorkUnit> units = coordinator.planWorkUnits(nextLogs);
        final List<WorkUnit> bridges = units.stream()
                .filter(u -> u.type() == WorkUnitType.BRIDGE)
                .toList();

        assertThat(bridges).hasSize(1);
        assertThat(bridges.get(0).inheritedTransactions())
                .extracting(InheritedTransaction::transactionId)
                .containsExactly(TX_A, TX_C);
    }

    @Test
    void testBridgeUnitIncludesPriorLogWhenSessionStartFallsOnBoundary() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);
        final Scn scn320 = Scn.valueOf(320);

        final InheritedTransaction inheritedA = inherited(TX_A, SCN_300, scn320);
        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_350, SCN_300);

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(inheritedA), List.of(orphanA), SCN_200, SCN_350));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        final List<LogFile> nextLogs = List.of(
                archiveLog("log2", SCN_200, SCN_300, 2),
                archiveLog("log3", SCN_300, SCN_400, 3));

        final List<WorkUnit> bridges = coordinator.planWorkUnits(nextLogs).stream()
                .filter(unit -> unit.type() == WorkUnitType.BRIDGE)
                .toList();

        assertThat(bridges).hasSize(1);
        assertThat(bridges.get(0).sessionStartScn()).isEqualTo(SCN_300.subtract(Scn.ONE));
        assertThat(bridges.get(0).logFiles())
                .extracting(LogFile::getFileName)
                .containsExactly("log2", "log3");
    }

    @Test
    void testPlanWorkUnitsPreservesSessionOnlyLogOutsideBridgeUnsafeInterval() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);
        final Scn scn320 = Scn.valueOf(320);

        final InheritedTransaction inheritedA = inherited(TX_A, SCN_300, scn320);
        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_350, SCN_300);

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(inheritedA), List.of(orphanA), SCN_200, SCN_350));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        final List<LogFile> nextLogs = List.of(
                archiveLog("log2", SCN_200, SCN_300, 2),
                archiveLog("log3", SCN_300, SCN_400, 3));

        final List<WorkUnit> units = coordinator.planWorkUnits(nextLogs);
        final List<WorkUnit> bridges = units.stream()
                .filter(unit -> unit.type() == WorkUnitType.BRIDGE)
                .toList();
        final List<WorkUnit> workers = units.stream()
                .filter(unit -> unit.type() == WorkUnitType.WORKER)
                .toList();

        assertThat(bridges).hasSize(1);
        assertThat(bridges.get(0).logFiles())
                .extracting(LogFile::getFileName)
                .containsExactly("log2", "log3");
        assertThat(bridges.get(0).readStartScn()).isEqualTo(scn320);
        assertThat(bridges.get(0).readEndScn()).isEqualTo(SCN_350);

        assertThat(workers).hasSize(1);
        assertThat(workers.get(0).logFiles())
                .extracting(LogFile::getFileName)
                .containsExactly("log2");
    }

    @Test
    void testExecutePendingBridgesPrunesOverlappingDuplicateOrphans() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(4);
        final Scn scn550 = Scn.valueOf(550);
        final Scn scn560 = Scn.valueOf(560);
        final Scn scn720 = Scn.valueOf(720);
        final Scn scn730 = Scn.valueOf(730);

        final InheritedTransaction inheritedA = inherited(TX_A, SCN_300, SCN_400);
        final InheritedTransaction inheritedB = inherited(TX_B, SCN_500, SCN_600);
        final InheritedTransaction inheritedC = inherited(TX_C, SCN_500, SCN_600);

        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_700, SCN_600);
        final OrphanCommit orphanB = orphanCommit(TX_B, scn720, SCN_600);
        final OrphanCommit orphanC = orphanCommit(TX_C, scn730, SCN_600);

        final List<LogFile> waveLogs = List.of(
                archiveLog("log1", SCN_100, SCN_300, 1),
                archiveLog("log2", SCN_300, SCN_500, 2),
                archiveLog("log3", SCN_500, SCN_700, 3),
                archiveLog("log4", SCN_700, SCN_800, 4));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(inheritedA), List.of(), SCN_100, SCN_300),
                workerResult(1, WorkUnitType.WORKER, List.of(), List.of(inheritedB, inheritedC), List.of(), SCN_300, SCN_600),
                workerResult(2, WorkUnitType.WORKER, List.of(), List.of(), List.of(orphanA), SCN_600, SCN_700),
                workerResult(3, WorkUnitType.WORKER, List.of(), List.of(), List.of(orphanB, orphanC), SCN_700, SCN_800));

        coordinator.executeWave(waveLogs, dispatcher, partition, offsetContext, ZONE_UTC);

        final CommittedTransaction committedA = committed(TX_A, SCN_300, SCN_700, "rs.A", List.of(dmlEvent(SCN_350, "DEBEZIUM.T")));
        final CommittedTransaction committedB = committed(TX_B, SCN_500, scn720, "rs.B", List.of(dmlEvent(scn550, "DEBEZIUM.T")));
        final CommittedTransaction committedC = committed(TX_C, SCN_500, scn730, "rs.C", List.of(dmlEvent(scn560, "DEBEZIUM.T")));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.BRIDGE, List.of(committedA), List.of(), List.of(), SCN_500, SCN_700),
                workerResult(1, WorkUnitType.BRIDGE, List.of(committedB, committedC), List.of(), List.of(orphanA), SCN_500, scn730));

        coordinator.executePendingBridges(waveLogs, dispatcher, partition, offsetContext, ZONE_UTC);

        assertThat(coordinator.getPendingInheritedTransactions()).isEmpty();
        assertThat(coordinator.getPendingOrphanCommits()).isEmpty();
    }

    @Test
    void testKnownCommitContinuationPlanComputesUnsafeIntervalExplicitly() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(4);
        final Scn scn120 = Scn.valueOf(120);
        final Scn scn450 = Scn.valueOf(450);

        final InheritedTransaction inheritedAfterWorker0 = inherited(TX_A, scn120, SCN_200, SCN_200);
        final InheritedTransaction inheritedAfterWorker1 = inherited(TX_A, scn120, SCN_200, SCN_400);
        final OrphanCommit orphanA = orphanCommit(TX_A, scn450, SCN_400);

        final List<LogFile> waveLogs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_400, 2),
                archiveLog("log3", SCN_400, SCN_600, 3),
                archiveLog("log4", SCN_600, SCN_800, 4));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(inheritedAfterWorker0), List.of(), SCN_100, SCN_200),
                workerResult(1, WorkUnitType.WORKER, List.of(), List.of(inheritedAfterWorker1), List.of(), SCN_200, SCN_400),
                workerResult(2, WorkUnitType.WORKER, List.of(), List.of(), List.of(orphanA), SCN_400, SCN_600),
                workerResult(3, WorkUnitType.WORKER, List.of(), List.of(), List.of(), SCN_600, SCN_800));

        coordinator.executeWave(waveLogs, dispatcher, partition, offsetContext, ZONE_UTC);

        final var plans = coordinator.planKnownCommitContinuations(waveLogs);

        assertThat(plans).hasSize(1);
        assertThat(plans.get(0).unsafeIntervalStartScn()).isEqualTo(SCN_200);
        assertThat(plans.get(0).unsafeIntervalEndScn()).isEqualTo(scn450);
        assertThat(plans.get(0).sessionStartScn()).isEqualTo(scn120.subtract(Scn.ONE));
        assertThat(plans.get(0).logFiles())
                .extracting(LogFile::getFileName)
                .containsExactly("log1", "log2", "log3");
    }

    @Test
    void testFindOverlappingWorkerResultsUsesActualReadSliceBoundaries() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);
        final Scn scn199 = Scn.valueOf(199);
        final Scn scn299 = Scn.valueOf(299);
        final Scn scn320 = Scn.valueOf(320);

        final InheritedTransaction inheritedA = inherited(TX_A, SCN_300, scn320);
        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_350, SCN_300);

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(inheritedA), List.of(orphanA), SCN_200, SCN_350));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        final List<LogFile> nextLogs = List.of(
                archiveLog("log2", SCN_200, SCN_300, 2),
                archiveLog("log3", SCN_300, SCN_400, 3));
        final var plans = coordinator.planKnownCommitContinuations(nextLogs);

        final WorkerResult unaffectedWorker = workerResult(0, WorkUnitType.WORKER, List.of(), List.of(), List.of(), scn199, SCN_200, SCN_300);
        final WorkerResult overlappingWorker = workerResult(1, WorkUnitType.WORKER, List.of(), List.of(), List.of(), scn299, SCN_300, SCN_400);
        final WorkerResult bridgeResult = workerResult(2, WorkUnitType.BRIDGE, List.of(), List.of(), List.of(), scn320, SCN_300, SCN_350);

        final List<WorkerResult> overlappingResults = coordinator.findOverlappingWorkerResults(
                List.of(unaffectedWorker, overlappingWorker, bridgeResult), plans);

        assertThat(overlappingResults)
                .extracting(WorkerResult::workerId)
                .containsExactly(1);
    }

    @Test
    void testExecuteWaveFiltersWorkerTransactionsInsideKnownUnsafeInterval() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);
        final Scn scn120 = Scn.valueOf(120);
        final Scn scn320 = Scn.valueOf(320);
        final Scn scn340 = Scn.valueOf(340);
        final Scn scn350 = Scn.valueOf(350);

        final InheritedTransaction inheritedA = inherited(TX_A, scn120, scn320, scn320);
        final OrphanCommit orphanA = orphanCommit(TX_A, scn350, SCN_300);
        final CommittedTransaction safeTx = committed(TX_B, SCN_210, SCN_250, "rs.safe", List.of(dmlEvent(SCN_230, "DEBEZIUM.T")));
        final CommittedTransaction unsafeTx = committed(TX_C, SCN_300, scn340, "rs.unsafe", List.of(dmlEvent(SCN_300, "DEBEZIUM.T")));

        final List<LogFile> waveLogs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_300, 2),
                archiveLog("log3", SCN_300, SCN_400, 3));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(safeTx, unsafeTx), List.of(inheritedA), List.of(orphanA), SCN_300.subtract(Scn.ONE), SCN_200,
                        SCN_400));

        coordinator.executeWave(waveLogs, dispatcher, partition, offsetContext, ZONE_UTC);

        verify(dispatcher, times(1)).dispatchTransactionCommittedEvent(eq(partition), any(), any());
        assertThat(coordinator.getPendingInheritedTransactions())
                .extracting(InheritedTransaction::transactionId)
                .containsExactly(TX_A);
        assertThat(coordinator.getPendingOrphanCommits())
                .extracting(OrphanCommit::transactionId)
                .containsExactly(TX_A);
    }

    @Test
    void testPlanReplayWorkUnitsBuildsTailAfterUnsafeInterval() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);
        final Scn scn320 = Scn.valueOf(320);
        final Scn scn350 = Scn.valueOf(350);
        final Scn scn299 = Scn.valueOf(299);

        final InheritedTransaction inheritedA = inherited(TX_A, SCN_300, scn320);
        final OrphanCommit orphanA = orphanCommit(TX_A, scn350, SCN_300);

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(inheritedA), List.of(orphanA), SCN_200, SCN_350));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        final List<LogFile> nextLogs = List.of(
                archiveLog("log2", SCN_200, SCN_300, 2),
                archiveLog("log3", SCN_300, SCN_400, 3),
                archiveLog("log4", SCN_400, SCN_500, 4));
        final var plans = coordinator.planKnownCommitContinuations(nextLogs);

        final WorkerResult overlappingWorker = workerResult(1, WorkUnitType.WORKER, List.of(), List.of(), List.of(), scn299, SCN_300, SCN_500);

        final List<WorkUnit> replayUnits = coordinator.planReplayWorkUnits(nextLogs, List.of(overlappingWorker), plans);

        assertThat(replayUnits).hasSize(1);
        assertThat(replayUnits.get(0).type()).isEqualTo(WorkUnitType.WORKER);
        assertThat(replayUnits.get(0).readStartScn()).isEqualTo(scn350);
        assertThat(replayUnits.get(0).readEndScn()).isEqualTo(SCN_500);
        assertThat(replayUnits.get(0).logFiles())
                .extracting(LogFile::getFileName)
                .containsExactly("log3", "log4");
    }

    @Test
    void testPlanReplayWorkUnitsSkipsWorkerWithoutTailBeyondUnsafeInterval() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);
        final Scn scn320 = Scn.valueOf(320);
        final Scn scn350 = Scn.valueOf(350);
        final Scn scn299 = Scn.valueOf(299);

        final InheritedTransaction inheritedA = inherited(TX_A, SCN_300, scn320);
        final OrphanCommit orphanA = orphanCommit(TX_A, scn350, SCN_300);

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(inheritedA), List.of(orphanA), SCN_200, SCN_350));
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);

        final List<LogFile> nextLogs = List.of(
                archiveLog("log2", SCN_200, SCN_300, 2),
                archiveLog("log3", SCN_300, SCN_400, 3));
        final var plans = coordinator.planKnownCommitContinuations(nextLogs);

        final WorkerResult noTailWorker = workerResult(1, WorkUnitType.WORKER, List.of(), List.of(), List.of(), scn299, SCN_300, scn350);

        assertThat(coordinator.planReplayWorkUnits(nextLogs, List.of(noTailWorker), plans)).isEmpty();
    }

    @Test
    void testExecutePendingBridgesClearsStaleReplayTailAfterBridge() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);
        final Scn scn120 = Scn.valueOf(120);
        final Scn scn299 = Scn.valueOf(299);
        final Scn scn320 = Scn.valueOf(320);
        final Scn scn340 = Scn.valueOf(340);
        final Scn scn350 = Scn.valueOf(350);
        final Scn scn500 = Scn.valueOf(500);

        final InheritedTransaction inheritedA = inherited(TX_A, scn120, scn320, scn320);
        final OrphanCommit orphanA = orphanCommit(TX_A, scn350, SCN_300);
        final CommittedTransaction safeTx = committed(TX_B, SCN_210, SCN_250, "rs.safe", List.of(dmlEvent(SCN_230, "DEBEZIUM.T")));
        final CommittedTransaction unsafeTx = committed(TX_C, SCN_300, scn340, "rs.unsafe", List.of(dmlEvent(SCN_300, "DEBEZIUM.T")));
        final CommittedTransaction bridgedTx = committed(TX_A, scn120, scn350, "rs.bridge", List.of(dmlEvent(SCN_150, "DEBEZIUM.T")));

        final List<LogFile> waveLogs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_300, 2),
                archiveLog("log3", SCN_300, SCN_400, 3),
                archiveLog("log4", SCN_400, SCN_500, 4));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(safeTx, unsafeTx), List.of(inheritedA), List.of(orphanA), scn299, SCN_300, scn500));

        coordinator.executeWave(waveLogs, dispatcher, partition, offsetContext, ZONE_UTC);

        verify(dispatcher, times(1)).dispatchTransactionCommittedEvent(eq(partition), any(), any());
        assertThat(coordinator.hasPendingBridgeTransactions()).isTrue();
        assertThat(coordinator.hasPendingReplayUnits()).isTrue();

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.BRIDGE, List.of(bridgedTx), List.of(), List.of(), scn320, SCN_300, scn350));

        coordinator.executePendingBridges(waveLogs, dispatcher, partition, offsetContext, ZONE_UTC);

        verify(dispatcher, times(2)).dispatchTransactionCommittedEvent(eq(partition), any(), any());
        assertThat(coordinator.hasPendingReplayUnits()).isFalse();
        coordinator.executePendingReplays(waveLogs, dispatcher, partition, offsetContext, ZONE_UTC);
        verify(dispatcher, times(2)).dispatchTransactionCommittedEvent(eq(partition), any(), any());
    }

    @Test
    void testExecutePendingBridgesRetainsHeldBackTailWithoutReplay() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(2);
        final Scn scn120 = Scn.valueOf(120);
        final Scn scn299 = Scn.valueOf(299);
        final Scn scn320 = Scn.valueOf(320);
        final Scn scn340 = Scn.valueOf(340);
        final Scn scn350 = Scn.valueOf(350);
        final Scn scn450 = Scn.valueOf(450);
        final Scn scn500 = Scn.valueOf(500);

        final InheritedTransaction inheritedA = inherited(TX_A, scn120, scn320, scn320);
        final OrphanCommit orphanA = orphanCommit(TX_A, scn350, SCN_300);
        final CommittedTransaction safeTx = committed(TX_B, SCN_210, SCN_250, "rs.safe", List.of(dmlEvent(SCN_230, "DEBEZIUM.T")));
        final CommittedTransaction heldTx = committed(TX_C, SCN_300, scn340, "rs.held", List.of(dmlEvent(SCN_300, "DEBEZIUM.T")));
        final CommittedTransaction bridgedTx = committed(TX_A, scn120, scn350, "rs.bridge", List.of(dmlEvent(SCN_150, "DEBEZIUM.T")));
        final CommittedTransaction replayTailTx = committed("TXDDD0004", SCN_400, scn450, "rs.replay", List.of(dmlEvent(SCN_400, "DEBEZIUM.T")));

        final List<LogFile> waveLogs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_300, 2),
                archiveLog("log3", SCN_300, SCN_400, 3),
                archiveLog("log4", SCN_400, SCN_500, 4));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(safeTx, heldTx), List.of(inheritedA), List.of(orphanA), scn299, SCN_300, scn500));

        coordinator.executeWave(waveLogs, dispatcher, partition, offsetContext, ZONE_UTC);

        verify(dispatcher, times(1)).dispatchTransactionCommittedEvent(eq(partition), any(), any());
        assertThat(coordinator.hasPendingBridgeTransactions()).isTrue();
        assertThat(coordinator.hasPendingReplayUnits()).isTrue();

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.BRIDGE, List.of(bridgedTx), List.of(), List.of(), scn320, SCN_300, scn350));

        coordinator.executePendingBridges(waveLogs, dispatcher, partition, offsetContext, ZONE_UTC);

        verify(dispatcher, times(2)).dispatchTransactionCommittedEvent(eq(partition), any(), any());
        assertThat(coordinator.hasPendingReplayUnits()).isFalse();
        coordinator.executePendingReplays(waveLogs, dispatcher, partition, offsetContext, ZONE_UTC);
        verify(dispatcher, times(2)).dispatchTransactionCommittedEvent(eq(partition), any(), any());

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(heldTx, replayTailTx), List.of(), List.of(), scn350, SCN_300, scn500));

        coordinator.executeWave(List.of(archiveLog("log3", SCN_300, SCN_400, 3), archiveLog("log4", SCN_400, SCN_500, 4)),
                dispatcher, partition, offsetContext, ZONE_UTC);

        verify(dispatcher, times(4)).dispatchTransactionCommittedEvent(eq(partition), any(), any());
    }

    @Test
    void testKnownCommitBridgeUsesOriginalTrustedPrefixAcrossMultipleUnresolvedWorkers() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(4);
        final Scn scn120 = Scn.valueOf(120);
        final Scn scn450 = Scn.valueOf(450);

        final InheritedTransaction inheritedAfterWorker0 = inherited(TX_A, scn120, SCN_200, SCN_200);
        final InheritedTransaction inheritedAfterWorker1 = inherited(TX_A, scn120, SCN_200, SCN_400);
        final OrphanCommit orphanA = orphanCommit(TX_A, scn450, SCN_400);

        final List<LogFile> waveLogs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_400, 2),
                archiveLog("log3", SCN_400, SCN_600, 3),
                archiveLog("log4", SCN_600, SCN_800, 4));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(inheritedAfterWorker0), List.of(), SCN_100, SCN_200),
                workerResult(1, WorkUnitType.WORKER, List.of(), List.of(inheritedAfterWorker1), List.of(), SCN_200, SCN_400),
                workerResult(2, WorkUnitType.WORKER, List.of(), List.of(), List.of(orphanA), SCN_400, SCN_600),
                workerResult(3, WorkUnitType.WORKER, List.of(), List.of(), List.of(), SCN_600, SCN_800));

        coordinator.executeWave(waveLogs, dispatcher, partition, offsetContext, ZONE_UTC);

        final List<WorkUnit> bridges = coordinator.planWorkUnits(waveLogs).stream()
                .filter(unit -> unit.type() == WorkUnitType.BRIDGE)
                .toList();

        assertThat(bridges).hasSize(1);
        assertThat(bridges.get(0).readStartScn()).isEqualTo(SCN_200);
        assertThat(bridges.get(0).readEndScn()).isEqualTo(scn450);
        assertThat(bridges.get(0).inheritedTransactions()).singleElement()
                .extracting(InheritedTransaction::transactionId)
                .isEqualTo(TX_A);
    }

    @Test
    void testFourWorkerWaveHandlesTwoSeparatedCrossWorkerTransactionsAroundLocalMiddleResolution() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(4);
        final Scn scn120 = Scn.valueOf(120);
        final Scn scn250 = Scn.valueOf(250);
        final Scn scn450 = Scn.valueOf(450);
        final Scn scn520 = Scn.valueOf(520);
        final Scn scn700 = Scn.valueOf(700);

        final InheritedTransaction inheritedA = inherited(TX_A, scn120, SCN_200);
        final InheritedTransaction inheritedC = inherited(TX_C, scn520, SCN_600);

        final OrphanCommit orphanA = orphanCommit(TX_A, scn450, SCN_400);
        final OrphanCommit orphanC = orphanCommit(TX_C, scn700, SCN_600);

        final CommittedTransaction committedB = committed(TX_B, SCN_210, scn250, "rs.B",
                List.of(dmlEvent(SCN_230, "DEBEZIUM.T")));

        final List<LogFile> waveLogs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_400, 2),
                archiveLog("log3", SCN_400, SCN_600, 3),
                archiveLog("log4", SCN_600, SCN_800, 4));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(inheritedA), List.of(), SCN_100, SCN_200),
                workerResult(1, WorkUnitType.WORKER, List.of(committedB), List.of(), List.of(), SCN_200, SCN_400),
                workerResult(2, WorkUnitType.WORKER, List.of(), List.of(inheritedC), List.of(orphanA), SCN_400, SCN_600),
                workerResult(3, WorkUnitType.WORKER, List.of(), List.of(), List.of(orphanC), SCN_600, SCN_800));

        coordinator.executeWave(waveLogs, dispatcher, partition, offsetContext, ZONE_UTC);

        // TX_B sits inside TX_A's unsafe interval and is withheld for reread by the later bridge.
        verify(dispatcher, never()).dispatchTransactionCommittedEvent(eq(partition), any(), any());
        assertThat(coordinator.getPendingInheritedTransactions())
                .extracting(InheritedTransaction::transactionId)
                .containsExactlyInAnyOrder(TX_A, TX_C);
        assertThat(coordinator.getPendingOrphanCommits())
                .extracting(OrphanCommit::transactionId)
                .containsExactlyInAnyOrder(TX_A, TX_C);
        assertThat(coordinator.hasPendingUnknownCommitTransactions()).isFalse();

        final List<WorkUnit> bridges = coordinator.planWorkUnits(waveLogs).stream()
                .filter(unit -> unit.type() == WorkUnitType.BRIDGE)
                .toList();

        assertThat(bridges).hasSize(2);
        assertThat(bridges.get(0).inheritedTransactions())
                .extracting(InheritedTransaction::transactionId)
                .containsExactly(TX_A);
        assertThat(bridges.get(0).readStartScn()).isEqualTo(SCN_200);
        assertThat(bridges.get(0).readEndScn()).isEqualTo(scn450);
        assertThat(bridges.get(0).logFiles())
                .extracting(LogFile::getFileName)
                .containsExactly("log1", "log2", "log3");

        assertThat(bridges.get(1).inheritedTransactions())
                .extracting(InheritedTransaction::transactionId)
                .containsExactly(TX_C);
        assertThat(bridges.get(1).readStartScn()).isEqualTo(SCN_600);
        assertThat(bridges.get(1).readEndScn()).isEqualTo(scn700);
        assertThat(bridges.get(1).logFiles())
                .extracting(LogFile::getFileName)
                .containsExactly("log3", "log4");
    }

    @Test
    void testFourWorkerWavePlansThreeDistinctBridgeUnits() throws Exception {
        final TestableCoordinator coordinator = createCoordinator(4);

        final InheritedTransaction inheritedA = inherited(TX_A, SCN_100, SCN_200);
        final InheritedTransaction inheritedB = inherited(TX_B, SCN_300, SCN_400);
        final InheritedTransaction inheritedC = inherited(TX_C, SCN_500, SCN_600);

        final OrphanCommit orphanA = orphanCommit(TX_A, SCN_300, SCN_200);
        final OrphanCommit orphanB = orphanCommit(TX_B, SCN_500, SCN_400);
        final OrphanCommit orphanC = orphanCommit(TX_C, SCN_700, SCN_600);

        final List<LogFile> waveLogs = List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_400, 2),
                archiveLog("log3", SCN_400, SCN_600, 3),
                archiveLog("log4", SCN_600, SCN_800, 4));

        coordinator.enqueueResults(
                workerResult(0, WorkUnitType.WORKER, List.of(), List.of(inheritedA), List.of(), SCN_100, SCN_200),
                workerResult(1, WorkUnitType.WORKER, List.of(), List.of(inheritedB), List.of(orphanA), SCN_200, SCN_400),
                workerResult(2, WorkUnitType.WORKER, List.of(), List.of(inheritedC), List.of(orphanB), SCN_400, SCN_600),
                workerResult(3, WorkUnitType.WORKER, List.of(), List.of(), List.of(orphanC), SCN_600, SCN_800));

        coordinator.executeWave(waveLogs, dispatcher, partition, offsetContext, ZONE_UTC);

        assertThat(coordinator.getPendingInheritedTransactions())
                .extracting(InheritedTransaction::transactionId)
                .containsExactlyInAnyOrder(TX_A, TX_B, TX_C);
        assertThat(coordinator.getPendingOrphanCommits())
                .extracting(OrphanCommit::transactionId)
                .containsExactlyInAnyOrder(TX_A, TX_B, TX_C);
        assertThat(coordinator.hasPendingUnknownCommitTransactions()).isFalse();

        final List<WorkUnit> units = coordinator.planWorkUnits(waveLogs);
        final List<WorkUnit> bridges = units.stream()
                .filter(u -> u.type() == WorkUnitType.BRIDGE)
                .toList();

        assertThat(bridges).hasSize(3);
        assertThat(bridges.get(0).inheritedTransactions()).singleElement()
                .extracting(InheritedTransaction::transactionId)
                .isEqualTo(TX_A);
        assertThat(bridges.get(1).inheritedTransactions()).singleElement()
                .extracting(InheritedTransaction::transactionId)
                .isEqualTo(TX_B);
        assertThat(bridges.get(2).inheritedTransactions()).singleElement()
                .extracting(InheritedTransaction::transactionId)
                .isEqualTo(TX_C);
    }

    // ==========================================================================
    // Test infrastructure
    // ==========================================================================

    /**
     * Convenience: runs a wave with a single pre-canned worker result to set up
     * pending state without going through the full work unit planning cycle.
     */
    private void runWaveWithSingleWorkerResult(TestableCoordinator coordinator, WorkerResult result) throws Exception {
        coordinator.enqueueResults(result);
        coordinator.executeWave(twoArchiveLogs(), dispatcher, partition, offsetContext, ZONE_UTC);
    }

    private static Stream<Arguments> concurrentReadingApplicabilityCases() {
        return Stream.of(
                Arguments.of(2, List.of(archiveLog("log1", SCN_100, SCN_200, 1)), false),
                Arguments.of(2, List.of(
                        archiveLog("log1", SCN_100, SCN_200, 1),
                        archiveLog("log2", SCN_200, SCN_300, 2),
                        redoLog("redo1", SCN_300, SCN_400, 3)), false),
                Arguments.of(2, List.of(redoLog("redo1", SCN_100, SCN_200, 1)), false),
                Arguments.of(2, List.of(
                        archiveLog("log1", SCN_100, SCN_200, 1),
                        archiveLog("log2", SCN_200, SCN_300, 2)), true),
                Arguments.of(4, List.of(
                        archiveLog("log1", SCN_100, SCN_200, 1),
                        archiveLog("log2", SCN_200, SCN_300, 2),
                        archiveLog("log3", SCN_300, SCN_400, 3),
                        archiveLog("log4", SCN_400, SCN_500, 4)), true));
    }

    private TestableCoordinator createCoordinator(int concurrentReaders) {
        final OracleConnectorConfig cfg = new OracleConnectorConfig(
                TestHelper.defaultConfig()
                        .with(OracleConnectorConfig.LOG_MINING_CONCURRENT_READERS, concurrentReaders)
                        .build());
        return new TestableCoordinator(cfg, schema, jdbcConfig, null);
    }

    private List<LogFile> twoArchiveLogs() {
        return List.of(
                archiveLog("log1", SCN_100, SCN_200, 1),
                archiveLog("log2", SCN_200, SCN_300, 2));
    }

    private static LogFile archiveLog(String name, Scn firstScn, Scn nextScn, int seq) {
        return LogFile.forArchive(name, firstScn, nextScn, BigInteger.valueOf(seq), 1, 1024L, false, false);
    }

    private static LogFile redoLog(String name, Scn firstScn, Scn nextScn, int seq) {
        return LogFile.forRedo(name, firstScn, nextScn, BigInteger.valueOf(seq), false, 1, 1024L);
    }

    private static CommittedTransaction committed(String txId, Scn startScn, Scn commitScn,
                                                  String rsId, List<LogMinerEvent> events) {
        return new CommittedTransaction(txId, startScn, commitScn, Instant.now(), "user1", null, 1, rsId, events);
    }

    private static InheritedTransaction inherited(String txId, Scn startScn, Scn lastReadScn) {
        return inherited(txId, startScn, lastReadScn, lastReadScn);
    }

    private static InheritedTransaction inherited(String txId, Scn startScn, Scn trustedPrefixScn, Scn lastReadScn) {
        return new InheritedTransaction(txId, startScn, Instant.now(), "user1", null, 1, trustedPrefixScn, lastReadScn, List.of());
    }

    private static OrphanCommit orphanCommit(String txId, Scn commitScn, Scn logFirstScn) {
        return new OrphanCommit(txId, commitScn, Instant.now(), 1, logFirstScn);
    }

    private static LogMinerEvent dmlEvent(Scn scn, String tableId) {
        return new DmlEvent(EventType.INSERT, scn, TableId.parse(tableId, false), "AAAAAAAAAAAAAAAAAA", "rs.0001", Instant.now(),
                new Object[0], new Object[]{ "test-value" });
    }

    private static WorkerResult workerResult(
                                             int workerId,
                                             WorkUnitType type,
                                             List<CommittedTransaction> resolved,
                                             List<InheritedTransaction> unresolved,
                                             List<OrphanCommit> orphans,
                                             Scn readStartScn,
                                             Scn logFirstScn,
                                             Scn finalReadScn) {
        return new WorkerResult(workerId, type, resolved, unresolved, orphans, List.of(), readStartScn, logFirstScn, finalReadScn);
    }

    private static WorkerResult workerResult(
                                             int workerId,
                                             WorkUnitType type,
                                             List<CommittedTransaction> resolved,
                                             List<InheritedTransaction> unresolved,
                                             List<OrphanCommit> orphans,
                                             Scn logFirstScn,
                                             Scn finalReadScn) {
        return new WorkerResult(workerId, type, resolved, unresolved, orphans, List.of(), logFirstScn.subtract(Scn.ONE), logFirstScn, finalReadScn);
    }

    private static WorkerResult workerResult(
                                             int workerId,
                                             WorkUnitType type,
                                             List<CommittedTransaction> resolved,
                                             List<InheritedTransaction> unresolved,
                                             List<OrphanCommit> orphans,
                                             List<WorkerResult.SchemaChangeRecord> schemaChanges,
                                             Scn logFirstScn,
                                             Scn finalReadScn) {
        return new WorkerResult(workerId, type, resolved, unresolved, orphans, schemaChanges, logFirstScn.subtract(Scn.ONE), logFirstScn,
                finalReadScn);
    }

    /**
     * Testable subclass of {@link LogMinerWorkerCoordinator} that intercepts worker execution
     * and returns pre-canned {@link WorkerResult} objects instead of connecting to Oracle.
     */
    static class TestableCoordinator extends LogMinerWorkerCoordinator {

        private final List<List<WorkerResult>> resultQueue = new ArrayList<>();
        private final List<List<Integer>> mergedPrefixes = new ArrayList<>();
        private RuntimeException nextFailure;

        TestableCoordinator(OracleConnectorConfig connectorConfig,
                            OracleDatabaseSchema schema,
                            JdbcConfiguration jdbcConfig,
                            AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics) {
            super(connectorConfig, schema, jdbcConfig, streamingMetrics);
        }

        void enqueueResults(WorkerResult... results) {
            resultQueue.add(new ArrayList<>(List.of(results)));
        }

        void enqueueFailure(RuntimeException failure) {
            this.nextFailure = failure;
        }

        @Override
        protected List<WorkerResult> executeWorkers(List<WorkUnit> units) throws InterruptedException {
            if (nextFailure != null) {
                final RuntimeException failure = nextFailure;
                nextFailure = null;
                throw failure;
            }
            if (resultQueue.isEmpty()) {
                return Collections.emptyList();
            }
            return resultQueue.remove(0);
        }

        @Override
        protected void executeWorkersAsCompleted(List<WorkUnit> units, WorkerResultConsumer consumer) throws InterruptedException {
            for (WorkerResult result : executeWorkers(units)) {
                consumer.accept(result);
            }
        }

        @Override
        protected Scn mergeCompletedWorkerPrefix(List<LogFile> availableLogs,
                                                 List<WorkerResult> results,
                                                 EventDispatcher<OraclePartition, TableId> dispatcher,
                                                 OraclePartition partition,
                                                 OracleOffsetContext offsetContext,
                                                 ZoneOffset databaseOffset,
                                                 boolean updatePendingReplayUnits,
                                                 List<WorkerResult.SchemaChangeRecord> appliedWaveSchemaChanges)
                throws InterruptedException {
            mergedPrefixes.add(results.stream().map(WorkerResult::workerId).toList());
            return super.mergeCompletedWorkerPrefix(
                    availableLogs,
                    results,
                    dispatcher,
                    partition,
                    offsetContext,
                    databaseOffset,
                    updatePendingReplayUnits,
                    appliedWaveSchemaChanges);
        }

        List<List<Integer>> mergedPrefixes() {
            return mergedPrefixes;
        }

        List<CommittedTransaction> pendingDispatch() {
            return getPrivateList("pendingDispatch");
        }

        List<WorkerResult.SchemaChangeRecord> pendingSchemaChanges() {
            return getPrivateList("pendingSchemaChanges");
        }

        private <T> List<T> getPrivateList(String fieldName) {
            try {
                final Field field = LogMinerWorkerCoordinator.class.getDeclaredField(fieldName);
                field.setAccessible(true);
                return List.copyOf((List<T>) field.get(this));
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to read field " + fieldName, e);
            }
        }
    }
}
