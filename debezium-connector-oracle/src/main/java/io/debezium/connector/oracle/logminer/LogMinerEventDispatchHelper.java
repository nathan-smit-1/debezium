/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.AbstractOracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleSchemaChangeEventEmitter;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.TruncateReceiver;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.events.RedoSqlDmlEvent;
import io.debezium.connector.oracle.logminer.events.TruncateEvent;
import io.debezium.data.Envelope;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

public final class LogMinerEventDispatchHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerEventDispatchHelper.class);

    private static final String NO_REDO_SQL_FOR_TEMPORARY_TABLES = "/* No SQL_REDO for temporary tables */";

    private LogMinerEventDispatchHelper() {
    }

    public static boolean isTransactionSkippedAtCommit(OracleConnectorConfig connectorConfig, String userName, String clientId) {
        return isUserNameSkipped(connectorConfig, userName) || isClientIdSkipped(connectorConfig, clientId);
    }

    public static boolean isUserNameSkipped(OracleConnectorConfig connectorConfig, String userName) {
        if (!Strings.isNullOrEmpty(userName)) {
            final Set<String> userNameExcludes = connectorConfig.getLogMiningUsernameExcludes();
            final Set<String> userNameIncludes = connectorConfig.getLogMiningUsernameIncludes();
            if (userNameExcludes.contains(userName)) {
                LOGGER.debug("Skipped transaction with excluded username {}", userName);
                return true;
            }
            else if (!userNameIncludes.isEmpty() && !userNameIncludes.contains(userName)) {
                LOGGER.debug("Skipped transaction with username {}", userName);
                return true;
            }
        }
        return false;
    }

    public static boolean isClientIdSkipped(OracleConnectorConfig connectorConfig, String clientId) {
        if (!Strings.isNullOrEmpty(clientId)) {
            final Set<String> clientIdExcludes = connectorConfig.getLogMiningClientIdExcludes();
            final Set<String> clientIdIncludes = connectorConfig.getLogMiningClientIdIncludes();
            if (clientIdExcludes.contains(clientId)) {
                LOGGER.debug("Skipped transaction with excluded client id {}", clientId);
                return true;
            }
            else if (!clientIdIncludes.isEmpty() && !clientIdIncludes.contains(clientId)) {
                LOGGER.debug("Skipped transaction with client id {}", clientId);
                return true;
            }
        }
        return false;
    }

    public static boolean isSchemaChangeEventSkipped(OracleConnectorConfig connectorConfig,
                                                     OracleDatabaseSchema schema,
                                                     OracleOffsetContext offsetContext,
                                                     LogMinerEventRow event) {
        final TableId tableId = event.getTableId();
        if (isSchemaChangeSkippedByUser(connectorConfig, event.getUserName())) {
            return true;
        }

        if (!Strings.isNullOrEmpty(event.getInfo()) && event.getInfo().startsWith("INTERNAL DDL")) {
            LOGGER.debug("Internal DDL skipped.");
            return true;
        }

        if (isSchemaChangeSkippedByTableFilter(connectorConfig, schema, tableId)) {
            return true;
        }

        if (offsetContext != null && offsetContext.getCommitScn().hasEventScnBeenHandled(event)) {
            final Scn commitScn = offsetContext.getCommitScn().getCommitScnForRedoThread(event.getThread());
            LOGGER.trace("DDL skipped with SCN {} <= Commit SCN {} for thread {}",
                    event.getScn(), commitScn, event.getThread());
            return true;
        }

        return tableId == null;
    }

    public static boolean isSchemaChangeSkippedByUser(OracleConnectorConfig connectorConfig, String userName) {
        final boolean skipByUser = connectorConfig.getLogMiningSchemaChangesUsernameExcludes().stream()
                .anyMatch(name -> name.equalsIgnoreCase(userName));
        if (skipByUser) {
            LOGGER.debug("User '{}' is in schema change exclusions, DDL skipped.", userName);
            return true;
        }
        return false;
    }

    public static boolean isSchemaChangeSkippedByTableFilter(OracleConnectorConfig connectorConfig,
                                                              OracleDatabaseSchema schema,
                                                              TableId tableId) {
        if (tableId != null && schema.storeOnlyCapturedTables()
                && !connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
            LOGGER.debug("Skipped DDL associated with table '{}' because schema history only stores included tables.", tableId);
            return true;
        }
        return false;
    }

    public static boolean isDmlEventEligible(LogMinerEventRow row) {
        if (row.hasErrorStatus() && !Strings.isNullOrBlank(row.getInfo())) {
            return false;
        }

        if (NO_REDO_SQL_FOR_TEMPORARY_TABLES.equals(row.getRedoSql())) {
            return false;
        }

        return true;
    }

    public static void dispatchDataChangeEvent(OracleConnectorConfig connectorConfig,
                                               EventDispatcher<OraclePartition, TableId> dispatcher,
                                               OraclePartition partition,
                                               OracleOffsetContext offsetContext,
                                               OracleDatabaseSchema schema,
                                               ZoneOffset databaseOffset,
                                               String transactionId,
                                               String userName,
                                               int redoThread,
                                               Scn commitScn,
                                               Instant commitTime,
                                               DmlEvent event,
                                               long eventIndex)
            throws InterruptedException {
        offsetContext.setEventScn(event.getScn());
        offsetContext.setEventCommitScn(commitScn);
        offsetContext.setTransactionId(transactionId);
        offsetContext.setTransactionSequence(eventIndex);
        offsetContext.setUserName(userName);
        offsetContext.setSourceTime(event.getChangeTime().minusSeconds(databaseOffset.getTotalSeconds()));
        offsetContext.setTableId(event.getTableId());
        offsetContext.setRedoThread(redoThread);
        offsetContext.setRsId(event.getRsId());
        offsetContext.setRowId(event.getRowIdAsString());
        offsetContext.setCommitTime(commitTime.minusSeconds(databaseOffset.getTotalSeconds()));

        if (eventIndex == 1L) {
            offsetContext.setStartScn(event.getScn());
            offsetContext.setStartTime(event.getChangeTime().minusSeconds(databaseOffset.getTotalSeconds()));
        }

        if (event instanceof RedoSqlDmlEvent redoSqlDmlEvent) {
            offsetContext.setRedoSql(redoSqlDmlEvent.getRedoSql());
        }

        final var table = schema.tableFor(event.getTableId());

        final DmlEvent dmlEvent = event;
        if (dmlEvent instanceof TruncateEvent) {
            dispatcher.dispatchDataChangeEvent(
                    partition,
                    event.getTableId(),
                    (ChangeRecordEmitter<OraclePartition>) new LogMinerChangeRecordEmitter(
                            connectorConfig,
                            partition,
                            offsetContext,
                            Envelope.Operation.TRUNCATE,
                            dmlEvent.getOldValues(),
                            dmlEvent.getNewValues(),
                            table,
                            schema,
                            Clock.system()));
        }
        else {
            dispatcher.dispatchDataChangeEvent(
                    partition,
                    event.getTableId(),
                    (ChangeRecordEmitter<OraclePartition>) new LogMinerChangeRecordEmitter(
                            connectorConfig,
                            partition,
                            offsetContext,
                            dmlEvent.getEventType(),
                            dmlEvent.getOldValues(),
                            dmlEvent.getNewValues(),
                            table,
                            schema,
                            Clock.system()));
        }
        offsetContext.setRedoSql(null);
    }

    public static void finishCommittedTransaction(EventDispatcher<OraclePartition, TableId> dispatcher,
                                                  OraclePartition partition,
                                                  OracleOffsetContext offsetContext,
                                                  int redoThread,
                                                  String transactionId,
                                                  Scn commitScn,
                                                  String commitRsId,
                                                  Instant commitTime,
                                                  boolean dispatchTransactionCommittedEvent)
            throws InterruptedException {
        final CommitScn offsetCommitScn = offsetContext.getCommitScn();
        if (offsetCommitScn != null) {
            offsetCommitScn.recordCommit(redoThread, commitScn, transactionId);
        }

        offsetContext.setEventScn(commitScn);
        offsetContext.setRsId(commitRsId);
        offsetContext.setRowId("");
        offsetContext.setStartScn(Scn.NULL);
        offsetContext.setCommitTime(null);
        offsetContext.setStartTime(null);

        if (dispatchTransactionCommittedEvent) {
            dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext, commitTime);
        }
        else {
            dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
        }
    }

    public static void dispatchSchemaChangeEvent(OracleConnectorConfig connectorConfig,
                                                 EventDispatcher<OraclePartition, TableId> dispatcher,
                                                 OraclePartition partition,
                                                 OracleOffsetContext offsetContext,
                                                 OracleDatabaseSchema schema,
                                                 AbstractOracleStreamingChangeEventSourceMetrics streamingMetrics,
                                                 TruncateReceiver truncateReceiver,
                                                 LogMinerSchemaChangeRecord change)
            throws InterruptedException {
        final TableId tableId = change.tableId();

        offsetContext.setEventScn(change.scn());
        offsetContext.setRedoThread(change.redoThread());
        offsetContext.setRsId(change.rsId());
        offsetContext.setRowId("");
        offsetContext.setTransactionSequence(change.transactionSequence());

        dispatcher.dispatchSchemaChangeEvent(
                partition,
                offsetContext,
                tableId,
                new OracleSchemaChangeEventEmitter(
                        connectorConfig,
                        partition,
                        offsetContext,
                        tableId,
                        tableId.catalog(),
                        tableId.schema(),
                        change.objectId(),
                        change.objectId(),
                        change.redoSql(),
                        schema,
                        change.changeTime(),
                        streamingMetrics,
                        truncateReceiver));
    }
}
