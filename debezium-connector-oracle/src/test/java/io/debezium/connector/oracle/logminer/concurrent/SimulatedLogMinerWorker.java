/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.concurrent;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogFile;
import io.debezium.connector.oracle.logminer.LogMinerEventFactory;
import io.debezium.connector.oracle.logminer.LogMinerSchemaChangeRecord;
import io.debezium.connector.oracle.logminer.LogMinerTransactionCacheHelper;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryTransaction;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryTransactionFactory;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;

/**
 * A test-only worker that reads simulated LogMiner events from an SQLite-backed
 * {@link LogMinerEventSimulator} instead of a real Oracle database.
 *
 * <p>The worker replicates the core transaction-tracking behavior of
 * {@link LogMinerWorker} but uses placeholder DML events (no SQL parsing) so
 * that tests can run without an Oracle connection or full table schema.
 *
 * @author Debezium Authors
 */
public class SimulatedLogMinerWorker implements Callable<WorkerResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimulatedLogMinerWorker.class);

    private final int workerId;
    private final WorkUnit unit;
    private final LogMinerEventSimulator simulator;
    private final OracleConnectorConfig connectorConfig;
    private final OracleDatabaseSchema schema;

    public SimulatedLogMinerWorker(int workerId,
                                   WorkUnit unit,
                                   LogMinerEventSimulator simulator,
                                   OracleConnectorConfig connectorConfig,
                                   OracleDatabaseSchema schema) {
        this.workerId = workerId;
        this.unit = unit;
        this.simulator = simulator;
        this.connectorConfig = connectorConfig;
        this.schema = schema;
    }

    @Override
    public WorkerResult call() throws Exception {
        LOGGER.info("Simulated worker {} starting: type={}, read=[{},{}], logs={}",
                workerId, unit.type(),
                unit.readStartScn(), unit.readEndScn(),
                unit.logFiles().stream().map(LogFile::getFileName).toList());

        final MemoryCacheProvider cacheProvider = new MemoryCacheProvider(connectorConfig);
        final MemoryTransactionFactory transactionFactory = new MemoryTransactionFactory();

        final List<CommittedTransaction> resolved = new ArrayList<>();
        final List<OrphanCommit> orphans = new ArrayList<>();
        final List<LogMinerSchemaChangeRecord> schemaChanges = new ArrayList<>();

        final Map<String, InheritedTransaction> inheritedByTxId = new HashMap<>();
        for (InheritedTransaction inherited : unit.inheritedTransactions()) {
            inheritedByTxId.put(inherited.transactionId(), inherited);
            final MemoryTransaction tx = new MemoryTransaction(
                    inherited.transactionId(),
                    inherited.startScn(),
                    inherited.startTime(),
                    inherited.userName(),
                    inherited.redoThreadId(),
                    inherited.clientId());
            cacheProvider.getTransactionCache().addTransaction(tx);
            int eventId = 0;
            for (LogMinerEvent event : inherited.events()) {
                cacheProvider.getTransactionCache().addTransactionEvent(tx, eventId++, event);
            }
            for (int i = 0; i < inherited.events().size(); i++) {
                tx.getNextEventId();
            }
        }

        final Scn logFirstScn = unit.logFiles().stream()
                .map(LogFile::getFirstScn)
                .min(Comparator.naturalOrder())
                .orElse(unit.readStartScn());

        final Scn queryReadStartScn = unit.type() == WorkUnitType.WORKER
                && unit.readStartScn().compareTo(logFirstScn) == 0
                        ? logFirstScn.subtract(Scn.ONE)
                        : unit.readStartScn();

        Scn maxScnSeen = Scn.NULL;
        try (ResultSet rs = simulator.query(unit.logFiles(), queryReadStartScn, unit.readEndScn())) {
            while (rs.next()) {
                final LogMinerEventRow row = createRowFromResultSet(rs);
                processRow(row, cacheProvider, transactionFactory, resolved, orphans, schemaChanges);
                final Scn rowScn = row.getScn();
                if (maxScnSeen.isNull() || rowScn.compareTo(maxScnSeen) > 0) {
                    maxScnSeen = rowScn;
                }
            }
        }

        final Scn finalReadScn = maxScnSeen.isNull() ? unit.readEndScn() : maxScnSeen;

        final List<InheritedTransaction> unresolved = buildUnresolved(cacheProvider, inheritedByTxId, finalReadScn);
        resolved.sort(Comparator.comparing(CommittedTransaction::commitScn));

        LOGGER.info("Simulated worker {} complete: resolved={}, unresolved={}, orphans={}, schemaChanges={}",
                workerId, resolved.size(), unresolved.size(), orphans.size(), schemaChanges.size());

        return new WorkerResult(workerId, unit.type(), resolved, unresolved, orphans, schemaChanges,
                unit.readStartScn(), logFirstScn, finalReadScn);
    }

    // ------------------------------------------------------------------ row processing

    private void processRow(LogMinerEventRow row,
                            MemoryCacheProvider cacheProvider,
                            MemoryTransactionFactory transactionFactory,
                            List<CommittedTransaction> resolved,
                            List<OrphanCommit> orphans,
                            List<LogMinerSchemaChangeRecord> schemaChanges)
            throws InterruptedException {

        final EventType type = row.getEventType();
        if (type == null) {
            return;
        }

        switch (type) {
            case START -> {
                LogMinerTransactionCacheHelper.startTransaction(cacheProvider.getTransactionCache(), transactionFactory, row);
            }
            case COMMIT -> {
                handleCommit(row, cacheProvider, resolved, orphans);
            }
            case ROLLBACK -> {
                LogMinerTransactionCacheHelper.removeTransactionAndEvents(cacheProvider.getTransactionCache(), row.getTransactionId());
            }
            case DDL -> {
                if (row.getTableId() != null) {
                    schemaChanges.add(LogMinerEventFactory.createSchemaChangeRecord(row));
                }
                final LogMinerEvent truncateEvent = LogMinerEventFactory.createTruncateEventIfDdl(row);
                if (truncateEvent != null) {
                    LogMinerTransactionCacheHelper.enqueueTransactionEvent(
                            cacheProvider.getTransactionCache(), transactionFactory, row, truncateEvent);
                }
            }
            case INSERT, UPDATE, DELETE -> {
                final LogMinerDmlEntry dmlEntry = createDmlEntry(row.getEventType());
                final LogMinerEvent event = new DmlEvent(row, dmlEntry);
                LogMinerTransactionCacheHelper.enqueueTransactionEvent(
                        cacheProvider.getTransactionCache(), transactionFactory, row, event);
            }
            default -> {
                // ignore unsupported / missing_scn / replication marker
            }
        }
    }

    private void handleCommit(LogMinerEventRow row,
                              MemoryCacheProvider cacheProvider,
                              List<CommittedTransaction> resolved,
                              List<OrphanCommit> orphans)
            throws InterruptedException {
        final String txId = row.getTransactionId();
        final MemoryTransaction tx = cacheProvider.getTransactionCache().getAndRemoveTransaction(txId);

        if (tx == null) {
            orphans.add(new OrphanCommit(
                    txId,
                    row.getScn(),
                    row.getChangeTime(),
                    row.getThread(),
                    unit.logFiles().stream()
                            .map(LogFile::getFirstScn)
                            .min(Comparator.naturalOrder())
                            .orElse(row.getScn())));
            return;
        }

        final List<LogMinerEvent> events = LogMinerTransactionCacheHelper.getActiveTransactionEvents(
                cacheProvider.getTransactionCache(), tx);
        cacheProvider.getTransactionCache().removeTransactionEvents(tx);

        resolved.add(new CommittedTransaction(
                txId,
                tx.getStartScn(),
                row.getScn(),
                row.getChangeTime(),
                tx.getUserName(),
                tx.getClientId(),
                tx.getRedoThreadId(),
                row.getRsId(),
                List.copyOf(events)));
    }

    private List<InheritedTransaction> buildUnresolved(MemoryCacheProvider cacheProvider,
                                                       Map<String, InheritedTransaction> inheritedByTxId,
                                                       Scn finalReadScn)
            throws InterruptedException {
        final List<InheritedTransaction> result = new ArrayList<>();
        cacheProvider.getTransactionCache().streamTransactionsAndReturn(stream -> {
            stream.forEach(tx -> {
                final List<LogMinerEvent> events;
                try {
                    events = LogMinerTransactionCacheHelper.getActiveTransactionEvents(
                            cacheProvider.getTransactionCache(), tx);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                final InheritedTransaction inherited = inheritedByTxId.get(tx.getTransactionId());
                result.add(new InheritedTransaction(
                        tx.getTransactionId(),
                        tx.getStartScn(),
                        tx.getChangeTime(),
                        tx.getUserName(),
                        tx.getClientId(),
                        tx.getRedoThreadId(),
                        inherited != null ? inherited.trustedPrefixScn() : finalReadScn,
                        finalReadScn,
                        List.copyOf(events)));
            });
            return null;
        });
        return result;
    }

    private static LogMinerDmlEntry createDmlEntry(EventType eventType) {
        return switch (eventType) {
            case INSERT -> LogMinerDmlEntryImpl.forInsert(new Object[0]);
            case UPDATE -> LogMinerDmlEntryImpl.forUpdate(new Object[0], new Object[0]);
            case DELETE -> LogMinerDmlEntryImpl.forDelete(new Object[0]);
            default -> LogMinerDmlEntryImpl.forValuelessDdl();
        };
    }

    // ------------------------------------------------------------------ reflection helpers

    /**
     * Creates a {@link LogMinerEventRow} from the SQLite result set.
     *
     * <p>SQLite stores the XID as TEXT, but {@link LogMinerEventRow#fromResultSet}
     * reads it via {@code ResultSet#getBytes}. That would produce a hex-encoded
     * representation of the UTF-8 bytes rather than the original string. This
     * method delegates to the standard factory and then fixes the
     * {@code transactionId} field via reflection.
     */
    private LogMinerEventRow createRowFromResultSet(ResultSet rs) throws SQLException {
        final LogMinerEventRow row = LogMinerEventRow.fromResultSet(rs, schema, connectorConfig);
        try {
            final Field transactionIdField = LogMinerEventRow.class.getDeclaredField("transactionId");
            transactionIdField.setAccessible(true);
            transactionIdField.set(row, rs.getString("XID"));
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to patch transactionId on LogMinerEventRow", e);
        }
        return row;
    }
}
