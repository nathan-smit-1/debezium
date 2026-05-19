/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.util.List;
import java.util.function.Consumer;

import io.debezium.connector.oracle.logminer.buffered.LogMinerTransactionCache;
import io.debezium.connector.oracle.logminer.buffered.Transaction;
import io.debezium.connector.oracle.logminer.buffered.TransactionFactory;
import io.debezium.connector.oracle.logminer.events.LogMinerEvent;
import io.debezium.connector.oracle.logminer.events.LogMinerEventRow;

public final class LogMinerTransactionCacheHelper {

    public static final String NO_SEQUENCE_TRX_ID_SUFFIX = "ffffffff";

    private LogMinerTransactionCacheHelper() {
    }

    public static <T extends Transaction> StartResult<T> startTransaction(LogMinerTransactionCache<T> transactionCache,
                                                                          TransactionFactory<T> transactionFactory,
                                                                          LogMinerEventRow event) {
        final T transaction = transactionCache.getTransaction(event.getTransactionId());
        if (transaction == null) {
            final T createdTransaction = transactionFactory.createTransaction(event);
            transactionCache.addTransaction(createdTransaction);
            return new StartResult<>(createdTransaction, true);
        }

        transactionCache.resetTransactionToStart(transaction);
        return new StartResult<>(transaction, false);
    }

    public static <T extends Transaction> EnqueueResult<T> enqueueTransactionEvent(LogMinerTransactionCache<T> transactionCache,
                                                                                   TransactionFactory<T> transactionFactory,
                                                                                   LogMinerEventRow event,
                                                                                   LogMinerEvent dispatchedEvent) {
        T transaction = transactionCache.getTransaction(event.getTransactionId());
        boolean created = false;
        if (transaction == null) {
            transaction = transactionFactory.createTransaction(event);
            transactionCache.addTransaction(transaction);
            created = true;
        }

        final int eventId = transaction.getNextEventId();
        boolean added = false;
        if (!transactionCache.containsTransactionEvent(transaction, eventId)) {
            transactionCache.addTransactionEvent(transaction, eventId, dispatchedEvent);
            added = true;
        }

        transactionCache.syncTransaction(transaction);
        return new EnqueueResult<>(transaction, created, added, eventId);
    }

    public static <T extends Transaction> T removeTransactionAndEvents(LogMinerTransactionCache<T> transactionCache, String transactionId) {
        final T transaction = transactionCache.getAndRemoveTransaction(transactionId);
        if (transaction != null) {
            transactionCache.removeTransactionEvents(transaction);
        }
        return transaction;
    }

    public static <T extends Transaction> List<LogMinerEvent> getActiveTransactionEvents(LogMinerTransactionCache<T> transactionCache,
                                                                                         T transaction)
            throws InterruptedException {
        final List<LogMinerEvent> events = new java.util.ArrayList<>();
        forEachActiveTransactionEvent(transactionCache, transaction, events::add);
        return events;
    }

    public static <T extends Transaction> void forEachActiveTransactionEvent(LogMinerTransactionCache<T> transactionCache,
                                                                             T transaction,
                                                                             Consumer<LogMinerEvent> consumer)
            throws InterruptedException {
        transactionCache.forEachEvent(transaction, (event, rolledBack) -> {
            if (!rolledBack) {
                consumer.accept(event);
            }
            return true;
        });
    }

    public static <T extends Transaction> List<T> findTransactionsByPrefix(LogMinerTransactionCache<T> transactionCache, String prefix) {
        return transactionCache.streamTransactionsAndReturn(stream -> stream.filter(transaction -> transaction.getTransactionId().startsWith(prefix))
                .toList());
    }

    public static boolean isPartialTransactionId(String transactionId) {
        return transactionId.endsWith(NO_SEQUENCE_TRX_ID_SUFFIX);
    }

    public static String getPartialTransactionPrefix(String transactionId) {
        return transactionId.substring(0, 8);
    }

    public record StartResult<T extends Transaction>(T transaction, boolean created) {
    }

    public record EnqueueResult<T extends Transaction>(T transaction, boolean created, boolean added, int eventId) {
    }
}