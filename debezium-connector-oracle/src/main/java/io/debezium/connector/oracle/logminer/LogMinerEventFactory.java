/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.time.Instant;
import java.util.Locale;
import java.util.function.Supplier;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.concurrent.WorkerResult;
import io.debezium.connector.oracle.logminer.events.DmlEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;
import io.debezium.connector.oracle.logminer.events.RedoSqlDmlEvent;
import io.debezium.connector.oracle.logminer.events.TruncateEvent;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.parser.LogMinerDmlEntryImpl;

public final class LogMinerEventFactory {

    private LogMinerEventFactory() {
    }

    public static LogMinerEvent createDataChangeEvent(LogMinerEventRow event, LogMinerDmlEntry parsedEvent, boolean includeRedoSql) {
        parsedEvent.setObjectName(event.getTableName());
        parsedEvent.setObjectOwner(event.getTablespaceName());

        if (includeRedoSql) {
            return new RedoSqlDmlEvent(event, parsedEvent, event.getRedoSql());
        }
        return new DmlEvent(event, parsedEvent);
    }

    public static LogMinerEvent createDataChangeEvent(LogMinerEventRow event,
                                                      Supplier<LogMinerDmlEntry> parsedEventSupplier,
                                                      boolean includeRedoSql) {
        final LogMinerDmlEntry parsedEvent = parsedEventSupplier.get();
        if (parsedEvent == null) {
            return null;
        }
        return createDataChangeEvent(event, parsedEvent, includeRedoSql);
    }

    public static TruncateEvent createTruncateEvent(LogMinerEventRow event) {
        final LogMinerDmlEntry parsedEvent = LogMinerDmlEntryImpl.forValuelessDdl();
        parsedEvent.setObjectName(event.getTableName());
        parsedEvent.setObjectOwner(event.getTablespaceName());
        return new TruncateEvent(event, parsedEvent);
    }

    public static TruncateEvent createTruncateEventIfDdl(LogMinerEventRow event) {
        if (event.getRedoSql() == null
                || !event.getRedoSql().trim().toUpperCase(Locale.ROOT).startsWith("TRUNCATE")) {
            return null;
        }
        return createTruncateEvent(event);
    }

    public static WorkerResult.SchemaChangeRecord createSchemaChangeRecord(LogMinerEventRow event) {
        return createSchemaChangeRecord(
                event.getScn(),
                event.getTableId(),
                event.getRedoSql(),
                event.getChangeTime(),
                event.getThread(),
                event.getRsId(),
                event.getTransactionSequence(),
                event.getObjectId());
    }

    public static WorkerResult.SchemaChangeRecord createSchemaChangeRecord(Scn scn,
                                                                           io.debezium.relational.TableId tableId,
                                                                           String redoSql,
                                                                           Instant changeTime,
                                                                           int redoThread,
                                                                           String rsId,
                                                                           long transactionSequence,
                                                                           Long objectId) {
        return new WorkerResult.SchemaChangeRecord(
                scn,
                tableId,
                redoSql,
                changeTime,
                redoThread,
                rsId,
                transactionSequence,
                objectId);
    }

}