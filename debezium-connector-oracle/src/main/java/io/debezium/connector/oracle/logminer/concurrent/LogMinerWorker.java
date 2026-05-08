/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.concurrent;

import static io.debezium.connector.oracle.logminer.concurrent.LogMinerWorkerDescriptions.describeLogs;
import static io.debezium.connector.oracle.logminer.concurrent.LogMinerWorkerDescriptions.describeTransactionIds;

import java.sql.PreparedStatement;
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

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogFile;
import io.debezium.connector.oracle.logminer.LogMinerSessionContext;
import io.debezium.connector.oracle.logminer.buffered.BufferedLogMinerQueryBuilder;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryCacheProvider;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryTransaction;
import io.debezium.connector.oracle.logminer.buffered.memory.MemoryTransactionFactory;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.EventType;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.events.TruncateEvent;
import io.debezium.connector.oracle.logminer.parser.DmlParserException;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlParser;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.util.Strings;

/**
 * A self-contained, isolated LogMiner reader that processes a single {@link WorkUnit}.
 *
 * <p>Each worker opens its own database connection, registers the assigned log files, starts a
 * LogMiner session bounded by {@link WorkUnit#sessionStartScn()} and
 * {@link WorkUnit#sessionEndScn()}, then queries {@code v$logmnr_contents} for
 * {@code scn > readStartScn AND scn <= readEndScn}.
 *
 * <p>The worker uses an in-memory transaction cache ({@link MemoryCacheProvider}) so that it
 * is isolated from the main connector's buffer and from other concurrent workers.
 *
 * <p>The worker returns a {@link WorkerResult} containing:
 * <ul>
 *   <li>resolved (committed) transactions found within the read window</li>
 *   <li>unresolved (open) transactions carried over to the next wave as
 *       {@link InheritedTransaction} records</li>
 *   <li>orphan commits ╬ô├ç├╢ COMMITs whose STARTs were not in either the inherited list or the
 *       read window</li>
 *   <li>schema-change records in SCN order</li>
 * </ul>
 *
 * @author Debezium Authors
 */
public class LogMinerWorker implements Callable<WorkerResult> {

    @FunctionalInterface
    public interface CommittedTransactionListener {
        void onCommittedTransaction(CommittedTransaction transaction) throws InterruptedException;
    }

    @FunctionalInterface
    public interface SchemaChangeListener {
        void onSchemaChange(WorkerResult.SchemaChangeRecord change) throws InterruptedException;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerWorker.class);

    private final int workerId;
    private final WorkUnit unit;
    private final OracleConnectorConfig connectorConfig;
    private final OracleDatabaseSchema schema;
    private final JdbcConfiguration jdbcConfig;
    private final String contentQuery;
    private final CommittedTransactionListener committedTransactionListener;
    private final SchemaChangeListener schemaChangeListener;

    public LogMinerWorker(int workerId,
                          WorkUnit unit,
                          OracleConnectorConfig connectorConfig,
                          OracleDatabaseSchema schema,
                          JdbcConfiguration jdbcConfig) {
        this(workerId, unit, connectorConfig, schema, jdbcConfig, null, null);
    }

    public LogMinerWorker(int workerId,
                          WorkUnit unit,
                          OracleConnectorConfig connectorConfig,
                          OracleDatabaseSchema schema,
                          JdbcConfiguration jdbcConfig,
                          CommittedTransactionListener committedTransactionListener,
                          SchemaChangeListener schemaChangeListener) {
        this.workerId = workerId;
        this.unit = unit;
        this.connectorConfig = connectorConfig;
        this.schema = schema;
        this.jdbcConfig = jdbcConfig;
        this.contentQuery = new BufferedLogMinerQueryBuilder(connectorConfig).getQuery() + " ORDER BY SCN, RS_ID";
        this.committedTransactionListener = committedTransactionListener;
        this.schemaChangeListener = schemaChangeListener;
    }

    @Override
    public WorkerResult call() throws Exception {
        LOGGER.info("Worker {} starting: type={}, session=[{},{}], read=[{},{}], logs={}, inheritedTransactions={}",
                workerId, unit.type(),
                unit.sessionStartScn(), unit.sessionEndScn(),
                unit.readStartScn(), unit.readEndScn(),
                describeLogs(unit.logFiles()),
                describeTransactionIds(unit.inheritedTransactions().stream()
                        .map(InheritedTransaction::transactionId)
                        .toList()));

        // Create an isolated in-memory cache for this worker
        final MemoryCacheProvider cacheProvider = new MemoryCacheProvider(connectorConfig);
        final MemoryTransactionFactory transactionFactory = new MemoryTransactionFactory();

        // Result accumulators
        final List<CommittedTransaction> resolved = new ArrayList<>();
        final List<OrphanCommit> orphans = new ArrayList<>();
        final List<WorkerResult.SchemaChangeRecord> schemaChanges = new ArrayList<>();

        // Pre-populate the cache with inherited transactions so that when their COMMITs
        // appear in this worker's read range the events are already available.
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
            // Re-add already-accumulated events so they are committed in order later
            int eventId = 0;
            for (LogMinerEvent event : inherited.events()) {
                cacheProvider.getTransactionCache().addTransactionEvent(tx, eventId++, event);
            }
            // Restore the event count so getNextEventId() continues from where it left off
            for (int i = 0; i < inherited.events().size(); i++) {
                tx.getNextEventId(); // advances numberOfEvents
            }
        }

        final LogMinerDmlParser dmlParser = new LogMinerDmlParser(connectorConfig);

        final Scn logFirstScn = unit.logFiles().stream()
                .map(LogFile::getFirstScn)
                .min(Comparator.naturalOrder())
                .orElse(unit.readStartScn());

        final Scn queryReadStartScn = unit.type() == WorkUnitType.WORKER
                && unit.readStartScn().compareTo(logFirstScn) == 0
                        ? logFirstScn.subtract(Scn.ONE)
                        : unit.readStartScn();

        try (var connection = createConnection()) {
            connection.setAutoCommit(false);

            try (LogMinerSessionContext sessionContext = new LogMinerSessionContext(
                    connection,
                    connectorConfig.isLogMiningContinuousMining(connection.getOracleVersion()),
                    connectorConfig.getLogMiningStrategy(),
                    connectorConfig.getLogMiningPathToDictionary())) {

                sessionContext.removeAllLogFilesFromSession();
                sessionContext.addLogFiles(unit.logFiles());
                sessionContext.startSession(unit.sessionStartScn(), unit.sessionEndScn(), false);

                try (PreparedStatement stmt = connection.connection().prepareStatement(
                        contentQuery,
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY,
                        ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
                    stmt.setFetchSize(connectorConfig.getQueryFetchSize());
                    stmt.setString(1, queryReadStartScn.toString());
                    stmt.setString(2, unit.readEndScn().toString());

                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            final LogMinerEventRow row = LogMinerEventRow.fromResultSet(rs, schema, connectorConfig);
                            processRow(row, cacheProvider, transactionFactory, dmlParser,
                                    inheritedByTxId, resolved, orphans, schemaChanges);
                        }
                    }
                }
            }
        }

        // Any transaction still in the cache is unresolved ╬ô├ç├╢ carry forward
        final List<InheritedTransaction> unresolved = buildUnresolved(cacheProvider, inheritedByTxId);

        // Sort resolved by commitScn ascending for merge-sort by coordinator
        resolved.sort(Comparator.comparing(CommittedTransaction::commitScn));

        LOGGER.info(
                "Worker {} complete: type={}, resolved={}, unresolved={}, orphans={}, schemaChanges={}, unresolvedTransactions={}, orphanTransactions={}",
                workerId,
                unit.type(),
                resolved.size(),
                unresolved.size(),
                orphans.size(),
                schemaChanges.size(),
                describeTransactionIds(unresolved.stream().map(InheritedTransaction::transactionId).toList()),
                describeTransactionIds(orphans.stream().map(OrphanCommit::transactionId).toList()));

        return new WorkerResult(workerId, unit.type(), resolved, unresolved, orphans, schemaChanges,
                unit.readStartScn(), logFirstScn, unit.readEndScn());
    }

    // ------------------------------- private helpers -----------------------------------------------

    private void processRow(LogMinerEventRow row,
                            MemoryCacheProvider cacheProvider,
                            MemoryTransactionFactory transactionFactory,
                            LogMinerDmlParser dmlParser,
                            Map<String, InheritedTransaction> inheritedByTxId,
                            List<CommittedTransaction> resolved,
                            List<OrphanCommit> orphans,
                            List<WorkerResult.SchemaChangeRecord> schemaChanges)
            throws InterruptedException {

        final EventType type = row.getEventType();

        if (type == null) {
            LOGGER.debug("Worker {} skipping unmapped LogMiner operation for transaction {} at SCN {}",
                    workerId, row.getTransactionId(), row.getScn());
            return;
        }

        switch (type) {
            case START -> {
                final String txId = row.getTransactionId();
                if (cacheProvider.getTransactionCache().getTransaction(txId) == null) {
                    cacheProvider.getTransactionCache().addTransaction(transactionFactory.createTransaction(row));
                }
                else {
                    cacheProvider.getTransactionCache().resetTransactionToStart(
                            cacheProvider.getTransactionCache().getTransaction(txId));
                }
            }
            case COMMIT -> handleCommit(row, cacheProvider, inheritedByTxId, resolved, orphans);
            case ROLLBACK -> {
                final MemoryTransaction tx = cacheProvider.getTransactionCache().getAndRemoveTransaction(row.getTransactionId());
                if (tx != null) {
                    cacheProvider.getTransactionCache().removeTransactionEvents(tx);
                }
            }
            case DDL -> {
                if (row.getTableId() != null) {
                    final WorkerResult.SchemaChangeRecord schemaChange = new WorkerResult.SchemaChangeRecord(
                            row.getScn(),
                            row.getTableId(),
                            row.getRedoSql(),
                            row.getChangeTime(),
                            row.getThread(),
                            row.getRsId(),
                            row.getTransactionSequence(),
                            row.getObjectId());
                    if (schemaChangeListener != null) {
                        schemaChangeListener.onSchemaChange(schemaChange);
                    }
                    else {
                        schemaChanges.add(schemaChange);
                    }
                    // TRUNCATE TABLE arrives as a DDL event. In addition to applying the schema
                    // change, we must record a TruncateEvent so the coordinator can dispatch it
                    // as a data-change event when the change is merged at wave end.
                    if (row.getRedoSql() != null
                            && row.getRedoSql().trim().toUpperCase(java.util.Locale.ROOT).startsWith("TRUNCATE")) {
                        final LogMinerDmlEntry entry = LogMinerDmlEntryImpl.forValuelessDdl();
                        entry.setObjectName(row.getTableName());
                        entry.setObjectOwner(row.getTablespaceName());
                        MemoryTransaction tx = cacheProvider.getTransactionCache().getTransaction(row.getTransactionId());
                        if (tx == null) {
                            tx = transactionFactory.createTransaction(row);
                            cacheProvider.getTransactionCache().addTransaction(tx);
                        }
                        final int eventId = tx.getNextEventId();
                        cacheProvider.getTransactionCache().addTransactionEvent(tx, eventId, new TruncateEvent(row, entry));
                    }
                }
            }
            case INSERT, UPDATE, DELETE -> handleDml(row, cacheProvider, transactionFactory, dmlParser);
            default -> {
                // Skip LOB, XML, unsupported etc. ╬ô├ç├╢ workers produce raw events only
                LOGGER.trace("Worker {} skipping event type {} at SCN {}", workerId, type, row.getScn());
            }
        }
    }

    private void handleDml(LogMinerEventRow row,
                           MemoryCacheProvider cacheProvider,
                           MemoryTransactionFactory transactionFactory,
                           LogMinerDmlParser dmlParser) {

        if (Strings.isNullOrBlank(row.getRedoSql())) {
            return;
        }
        if (row.getTableId() == null) {
            return;
        }

        final var table = schema.tableFor(row.getTableId());
        if (table == null) {
            return;
        }

        try {
            final var dmlEntry = dmlParser.parse(row.getRedoSql(), table);
            if (dmlEntry == null) {
                return;
            }

            MemoryTransaction tx = cacheProvider.getTransactionCache().getTransaction(row.getTransactionId());
            if (tx == null) {
                tx = transactionFactory.createTransaction(row);
                cacheProvider.getTransactionCache().addTransaction(tx);
            }

            final int eventId = tx.getNextEventId();
            cacheProvider.getTransactionCache().addTransactionEvent(tx, eventId,
                    new DmlEvent(row, dmlEntry));
        }
        catch (DmlParserException e) {
            LOGGER.warn("Worker {} failed to parse DML at SCN {} for table {}: {}",
                    workerId, row.getScn(), row.getTableId(), e.getMessage());
        }
    }

    private void handleCommit(LogMinerEventRow row,
                              MemoryCacheProvider cacheProvider,
                              Map<String, InheritedTransaction> inheritedByTxId,
                              List<CommittedTransaction> resolved,
                              List<OrphanCommit> orphans)
            throws InterruptedException {

        final String txId = row.getTransactionId();
        final MemoryTransaction tx = cacheProvider.getTransactionCache().getAndRemoveTransaction(txId);

        if (tx == null) {
            // We have no record of this transaction's START ╬ô├ç├╢ it is an orphan commit
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

        // Collect events from cache
        final List<LogMinerEvent> events = new ArrayList<>();
        try {
            cacheProvider.getTransactionCache().forEachEvent(tx, (event, rolledBack) -> {
                if (!rolledBack) {
                    events.add(event);
                }
                return true;
            });
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Worker {} interrupted while collecting events for transaction {}", workerId, txId);
        }

        cacheProvider.getTransactionCache().removeTransactionEvents(tx);

        // Determine transaction metadata from either cache or inherited record
        final String userName = tx.getUserName();
        final String clientId = tx.getClientId();
        final int redoThreadId = tx.getRedoThreadId();

        final CommittedTransaction committedTransaction = new CommittedTransaction(
                txId,
                tx.getStartScn(),
                row.getScn(),
                row.getChangeTime(),
                userName,
                clientId,
                redoThreadId,
                row.getRsId(),
                List.copyOf(events));

        if (committedTransactionListener != null) {
            committedTransactionListener.onCommittedTransaction(committedTransaction);
        }
        else {
            resolved.add(committedTransaction);
        }
    }

    private List<InheritedTransaction> buildUnresolved(MemoryCacheProvider cacheProvider,
                                                       Map<String, InheritedTransaction> inheritedByTxId) {
        final List<InheritedTransaction> result = new ArrayList<>();
        cacheProvider.getTransactionCache().streamTransactionsAndReturn(stream -> {
            stream.forEach(tx -> {
                final List<LogMinerEvent> events = new ArrayList<>();
                try {
                    cacheProvider.getTransactionCache().forEachEvent(tx, (event, rolledBack) -> {
                        if (!rolledBack) {
                            events.add(event);
                        }
                        return true;
                    });
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // The events list is partial; skip this transaction rather than
                    // forwarding incomplete state.
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
                        inherited != null ? inherited.trustedPrefixScn() : unit.readEndScn(),
                        unit.readEndScn(),
                        List.copyOf(events)));
            });
            return null;
        });
        return result;
    }

    private OracleConnection createConnection() throws SQLException {
        final OracleConnection connection = new OracleConnection(jdbcConfig, false);
        prepareMiningSession(connection);
        return connection;
    }

    private void prepareMiningSession(OracleConnection connection) throws SQLException {
        connection.executeWithoutCommitting("ALTER SESSION SET"
                + "  NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
                + "  NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'"
                + "  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM'"
                + "  NLS_NUMERIC_CHARACTERS = '.,'");
        connection.executeWithoutCommitting("ALTER SESSION SET TIME_ZONE = '00:00'");

        final long hashAreaSize = connectorConfig.getLogMiningHashAreaSize();
        if (hashAreaSize > 0) {
            connection.executeWithoutCommitting("ALTER SESSION SET HASH_AREA_SIZE = " + hashAreaSize);
        }

        final long sortAreaSize = connectorConfig.getLogMiningSortAreaSize();
        if (sortAreaSize > 0) {
            connection.executeWithoutCommitting("ALTER SESSION SET SORT_AREA_SIZE = " + sortAreaSize);
        }
    }

}
