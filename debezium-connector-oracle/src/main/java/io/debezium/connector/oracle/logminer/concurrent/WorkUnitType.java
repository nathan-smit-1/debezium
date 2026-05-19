/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.concurrent;

/**
 * The type of work unit submitted to a {@link LogMinerWorker}.
 *
 * @author Debezium Authors
 */
public enum WorkUnitType {

    /**
     * A clean, independent log slice. No inherited transactions.
     * The session window and read window cover only the assigned logs.
     */
    WORKER,

    /**
     * A cross-log resolution unit. Has inherited transactions whose STARTs are in prior logs.
     * The session window reaches back to cover those STARTs (Golden Rule).
     * The read window starts at the first SCN of the first "new" log in this unit (no re-reading).
     * {@code readEndScn} is a known tight cap equal to the max orphan commitScn from prior workers.
     */
    BRIDGE
}
