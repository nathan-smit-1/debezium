/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.concurrent;

import static io.debezium.connector.oracle.logminer.concurrent.LogMinerWorkerDescriptions.describeLogs;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.AbstractLogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.logminer.LogFile;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * A {@link AbstractLogMinerStreamingChangeEventSource} implementation that mines Oracle archive logs
 * concurrently using a pool of {@link LogMinerWorker} instances coordinated by a
 * {@link LogMinerWorkerCoordinator}.
 *
 * <p>Each wave of archive logs is partitioned into {@link WorkUnit}s which are executed in parallel.
 * Cross-log transactions (whose START is in one log and COMMIT is in another) are handled by
 * BRIDGE work units when the commit location is known, and by serial continuation when it is not.
 *
 * <p>This class falls back to the coordinator's single-threaded path when the activation gates
 * are not satisfied (e.g. online redo logs are in scope, or only one archive log is available).
 * In that case the coordinator still handles dispatch, but with a single worker.
 *
 * <p>This class is created only when {@code log.mining.concurrent.readers > 1}.
 *
 * @author Debezium Authors
 */
public class ConcurrentBufferedLogMinerStreamingChangeEventSource
        extends AbstractLogMinerStreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentBufferedLogMinerStreamingChangeEventSource.class);

    private final LogMinerWorkerCoordinator coordinator;

    /** Tracks whether the most recent wave dispatched any data. Updated after each wave iteration. */
    private volatile boolean dataDispatchedInCurrentWave = true;

    public ConcurrentBufferedLogMinerStreamingChangeEventSource(
                                                                OracleConnectorConfig connectorConfig,
                                                                OracleConnection jdbcConnection,
                                                                EventDispatcher<OraclePartition, TableId> dispatcher,
                                                                ErrorHandler errorHandler,
                                                                Clock clock,
                                                                OracleDatabaseSchema schema,
                                                                Configuration jdbcConfig,
                                                                LogMinerStreamingChangeEventSourceMetrics streamingMetrics) {
        super(connectorConfig, jdbcConnection, dispatcher, errorHandler, clock, schema, jdbcConfig, streamingMetrics);

        this.coordinator = new LogMinerWorkerCoordinator(
                connectorConfig,
                schema,
                JdbcConfiguration.adapt(jdbcConfig),
                streamingMetrics);
    }

    // ------------------------------------------------------------------ main mining loop

    @Override
    protected void executeLogMiningStreaming() throws Exception {
        LOGGER.info("Starting concurrent LogMiner streaming with {} reader threads",
                getConfig().getLogMiningConcurrentReaders());

        Scn currentReadScn = getOffsetContext().getScn();
        int waveNumber = 0;

        while (getContext().isRunning()) {
            waveNumber++;
            // In archive-log-only mode, wait until the range is available
            if (getConfig().isArchiveLogOnlyMode()) {
                if (waitForRangeAvailabilityInArchiveLogs(currentReadScn, Scn.NULL)) {
                    break;
                }
            }

            final Instant batchStartTime = Instant.now();

            final var miningWindow = prepareBoundedLogMiningWindow(currentReadScn, currentReadScn);
            if (miningWindow.isEmpty()) {
                LOGGER.debug("Upper bound calculation requested delay; pausing.");
                pauseBetweenMiningSessions();
                dataDispatchedInCurrentWave = false;
                continue;
            }

            final BoundedLogMiningWindow boundedWindow = miningWindow.get();
            final Scn upperBound = boundedWindow.upperBoundScn();
            final Scn finalUpperBound = boundedWindow.finalUpperBoundScn();
            final List<LogFile> allLogs = boundedWindow.logFiles();

            LOGGER.info("Concurrent LogMiner wave {} prepared: readScn={}, upperBound={}, finalUpperBound={}, logs={}",
                    waveNumber, currentReadScn, upperBound, finalUpperBound, describeLogs(allLogs));

            if (allLogs == null || allLogs.isEmpty()) {
                LOGGER.debug("No logs available in range [{}, {}]; pausing.", currentReadScn, finalUpperBound);
                pauseBetweenMiningSessions();
                dataDispatchedInCurrentWave = false;
                continue;
            }

            // Resolve the database timezone offset (updated by updateDatabaseTimeDifference above)
            final ZoneOffset databaseOffset = getMetrics().getDatabaseOffset();

            // Split logs: archive logs can be mined concurrently; online redo logs
            // must be mined serially because they are still being written to.
            final List<LogFile> archiveLogs = new ArrayList<>();
            final List<LogFile> redoLogs = new ArrayList<>();
            for (LogFile log : allLogs) {
                if (log.isArchive()) {
                    archiveLogs.add(log);
                }
                else if (log.isRedo()) {
                    redoLogs.add(log);
                }
            }

            final boolean hasUnknownCommitTransactions = coordinator.hasPendingUnknownCommitTransactions();

            // Activation gate: concurrent reading requires 2+ archive logs and no inherited
            // transactions whose commit location is still unknown.
            final boolean canRunConcurrent = archiveLogs.size() >= 2 && !hasUnknownCommitTransactions;

            if (canRunConcurrent) {
                // Phase 1: mine archive logs concurrently
                LOGGER.info("Concurrent LogMiner wave {} executing in concurrent mode over archive logs {}",
                        waveNumber, describeLogs(archiveLogs));
                Scn processedScn = coordinator.executeWave(
                        currentReadScn,
                        archiveLogs,
                        getEventDispatcher(),
                        getPartition(),
                        getOffsetContext(),
                        databaseOffset);
                while (coordinator.hasPendingBridgeTransactions() || coordinator.hasPendingReplayUnits()
                        || coordinator.hasPendingDdlContaminationReplay()) {
                    if (coordinator.hasPendingBridgeTransactions()) {
                        LOGGER.info("Concurrent LogMiner wave {} executing bridge follow-up pass over archive logs {}",
                                waveNumber, describeLogs(archiveLogs));
                        final Scn bridgeProcessedScn = coordinator.executePendingBridges(
                                archiveLogs,
                                getEventDispatcher(),
                                getPartition(),
                                getOffsetContext(),
                                databaseOffset);
                        if (processedScn.isNull() || (!bridgeProcessedScn.isNull() && bridgeProcessedScn.compareTo(processedScn) > 0)) {
                            processedScn = bridgeProcessedScn;
                        }
                        continue;
                    }

                    if (coordinator.hasPendingReplayUnits()) {
                        LOGGER.info("Concurrent LogMiner wave {} executing replay follow-up pass over archive logs {}",
                                waveNumber, describeLogs(archiveLogs));
                        final Scn replayProcessedScn = coordinator.executePendingReplays(
                                archiveLogs,
                                getEventDispatcher(),
                                getPartition(),
                                getOffsetContext(),
                                databaseOffset);
                        if (processedScn.isNull() || (!replayProcessedScn.isNull() && replayProcessedScn.compareTo(processedScn) > 0)) {
                            processedScn = replayProcessedScn;
                        }
                    }
                    else {
                        // DDL-contamination replay: re-dispatch quarantined transactions after
                        // the wave's schema changes have been applied to the internal schema model.
                        LOGGER.info("Concurrent LogMiner wave {} executing DDL-contamination replay",
                                waveNumber);
                        final Scn ddlReplayScn = coordinator.executeDdlContaminationReplay(
                                getEventDispatcher(),
                                getPartition(),
                                getOffsetContext(),
                                databaseOffset);
                        if (processedScn.isNull() || (!ddlReplayScn.isNull() && ddlReplayScn.compareTo(processedScn) > 0)) {
                            processedScn = ddlReplayScn;
                        }
                    }
                }
                if (!processedScn.isNull()) {
                    currentReadScn = processedScn;
                }

                // Phase 2: if online redo logs are in scope, process them serially as a
                // follow-up. The serial worker inherits any open transactions from the
                // concurrent archive phase so that cross-phase resolution works correctly.
                if (!redoLogs.isEmpty()) {
                    LOGGER.info("Concurrent LogMiner wave {} executing serial follow-up over redo logs {} with upperBound={}",
                            waveNumber, describeLogs(redoLogs), finalUpperBound);
                    final Scn redoProcessedScn = executeBoundedSerial(
                            currentReadScn,
                            finalUpperBound,
                            redoLogs,
                            databaseOffset);
                    if (!redoProcessedScn.isNull()) {
                        currentReadScn = redoProcessedScn;
                    }
                }

                if (!currentReadScn.isNull()) {
                    getOffsetContext().setScn(currentReadScn);
                    dataDispatchedInCurrentWave = true;
                    LOGGER.info("Concurrent LogMiner wave {} completed: processedScn={}",
                            waveNumber, currentReadScn);
                }
                else {
                    dataDispatchedInCurrentWave = false;
                    LOGGER.info("Concurrent LogMiner wave {} completed with no processed SCN advance",
                            waveNumber);
                }
            }
            else {
                // Fall back to serial mode: one worker processes all logs on the calling thread.
                // This is used when fewer than 2 archive logs are available, or when an inherited
                // transaction still has an unknown commit location and one worker must own
                // continuation until it resolves.
                if (hasUnknownCommitTransactions) {
                    LOGGER.info(
                            "Concurrent LogMiner wave {} falling back to serial mode over logs {} because inherited transactions {} still have unknown commit locations",
                            waveNumber,
                            describeLogs(allLogs),
                            coordinator.getPendingInheritedTransactions().stream()
                                    .map(InheritedTransaction::transactionId)
                                    .toList());
                }
                else {
                    LOGGER.info("Concurrent LogMiner wave {} falling back to serial mode over logs {}",
                            waveNumber, describeLogs(allLogs));
                }
                final Scn processedScn = executeBoundedSerial(
                        currentReadScn,
                        finalUpperBound,
                        allLogs,
                        databaseOffset);
                if (!processedScn.isNull()) {
                    currentReadScn = processedScn;
                    getOffsetContext().setScn(currentReadScn);
                    dataDispatchedInCurrentWave = true;
                    LOGGER.info("Concurrent LogMiner wave {} completed in serial mode: processedScn={}",
                            waveNumber, processedScn);
                }
                else {
                    dataDispatchedInCurrentWave = false;
                    LOGGER.info("Concurrent LogMiner wave {} completed in serial mode with no processed SCN advance",
                            waveNumber);
                }
            }

            getMetrics().setLastBatchProcessingDuration(Duration.between(batchStartTime, Instant.now()));

            captureJdbcSessionMemoryStatistics();
            pauseBetweenMiningSessions();

            if (getContext().isPaused()) {
                executeBlockingSnapshot();
            }
        }
    }

    private Scn executeBoundedSerial(Scn readStartScn,
                                     Scn readEndScn,
                                     List<LogFile> logs,
                                     ZoneOffset databaseOffset) {
        return coordinator.executeSerial(
                readStartScn,
                readEndScn,
                logs,
                getEventDispatcher(),
                getPartition(),
                getOffsetContext(),
                databaseOffset);
    }

    // ------------------------------------------------------------------ required abstract method implementations

    @Override
    protected void handleStartEvent(LogMinerEventRow event) throws InterruptedException {
        // Workers handle all events internally; this path should not be called.
        LOGGER.trace("handleStartEvent called on coordinator source (unexpected); ignoring SCN {}", event.getScn());
    }

    @Override
    protected void handleCommitEvent(LogMinerEventRow event) throws InterruptedException {
        // Workers handle all events internally; this path should not be called.
        LOGGER.trace("handleCommitEvent called on coordinator source (unexpected); ignoring SCN {}", event.getScn());
    }

    @Override
    protected void handleRollbackEvent(LogMinerEventRow event) throws InterruptedException {
        // Workers handle all events internally; this path should not be called.
        LOGGER.trace("handleRollbackEvent called on coordinator source (unexpected); ignoring SCN {}", event.getScn());
    }

    @Override
    protected void handleSchemaChangeEvent(LogMinerEventRow event) throws InterruptedException {
        // Workers handle all events internally; schema changes dispatched by coordinator.
        LOGGER.trace("handleSchemaChangeEvent called on coordinator source (unexpected); ignoring SCN {}", event.getScn());
    }

    @Override
    protected void handleTruncateEvent(LogMinerEventRow event) throws InterruptedException {
        // Workers handle all events internally; this path should not be called.
        LOGGER.trace("handleTruncateEvent called on coordinator source (unexpected); ignoring SCN {}", event.getScn());
    }

    @Override
    protected void handleReplicationMarkerEvent(LogMinerEventRow event) throws InterruptedException {
        // Workers handle all events internally; this path should not be called.
        LOGGER.trace("handleReplicationMarkerEvent called on coordinator source (unexpected); ignoring SCN {}", event.getScn());
    }

    @Override
    protected void enqueueEvent(LogMinerEventRow event, LogMinerEvent dispatchedEvent) throws InterruptedException {
        // Workers handle all events internally; this path should not be called.
        LOGGER.trace("enqueueEvent called on coordinator source (unexpected); ignoring SCN {}", event.getScn());
    }

    @Override
    protected List<String> getActiveTransactionIds() {
        return coordinator.getPendingInheritedTransactions().stream()
                .map(InheritedTransaction::transactionId)
                .toList();
    }

    @Override
    protected boolean isNoDataProcessedInBatchAndAtEndOfArchiveLogs() {
        return !dataDispatchedInCurrentWave;
    }

    @Override
    public void close() {
        try {
            coordinator.close();
        }
        catch (Exception e) {
            LOGGER.warn("Failed to close LogMiner worker coordinator", e);
        }
    }
}
