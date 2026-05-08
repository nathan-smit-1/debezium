/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.time.Instant;
import java.time.ZoneOffset;

import io.debezium.connector.oracle.AbstractOracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.CommitScn;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.OracleSchemaChangeEventEmitter;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.TruncateReceiver;
import io.debezium.connector.oracle.logminer.concurrent.WorkerResult;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.RedoSqlDmlEvent;
import io.debezium.connector.oracle.logminer.events.TruncateEvent;
import io.debezium.data.Envelope;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public final class LogMinerEventDispatchHelper {

    private LogMinerEventDispatchHelper() {
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

        offsetContext.setScn(commitScn);
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
                                                 WorkerResult.SchemaChangeRecord change)
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
