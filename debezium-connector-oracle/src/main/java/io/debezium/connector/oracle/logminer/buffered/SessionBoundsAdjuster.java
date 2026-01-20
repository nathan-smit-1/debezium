/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.buffered;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.SqlUtils;

/**
 * Manages session bounds adjustment for Oracle LogMiner to handle long-running transactions.
 * <p>
 * This component advances the mining session lower bound past transactions that exceed the
 * configured age threshold, preventing LogMiner queries from becoming increasingly expensive.
 *
 * @author Debezium Authors
 */
public class SessionBoundsAdjuster {

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionBoundsAdjuster.class);

    private final long thresholdMs;
    private final long checkIntervalMs;
    private final LogMinerStreamingChangeEventSourceMetrics metrics;

    private Instant nextCheckTime = Instant.MIN;

    public SessionBoundsAdjuster(long thresholdMs, LogMinerStreamingChangeEventSourceMetrics metrics) {
        this.thresholdMs = thresholdMs;
        this.checkIntervalMs = thresholdMs / 4;
        this.metrics = metrics;
    }

    /**
     * Calculates a new lower bound if long-running transactions exceed the threshold.
     *
     * @param connection the Oracle connection
     * @param cache the transaction cache
     * @param sessionEndScn current session upper bound
     * @param lastProcessedScn fallback SCN if all transactions exceed threshold
     * @return new lower bound SCN, or empty if no adjustment needed
     */
    public Optional<Scn> getNewLowerBound(OracleConnection connection,
                                          LogMinerTransactionCache<?> cache,
                                          Scn sessionEndScn,
                                          Scn lastProcessedScn) {
        // Layer 1: Quick metrics check - does oldest transaction exceed threshold?
        if (metrics.getOldestScnAgeInMilliseconds() <= thresholdMs) {
            return Optional.empty();
        }

        // Layer 2: Rate limiting - avoid expensive checks too frequently
        Instant now = Instant.now();
        if (now.isBefore(nextCheckTime)) {
            return Optional.empty();
        }
        nextCheckTime = now.plusMillis(checkIntervalMs);

        // Layer 3: Calculate threshold SCN and find new lower bound
        return calculateNewLowerBound(connection, cache, sessionEndScn, lastProcessedScn);
    }

    private Optional<Scn> calculateNewLowerBound(OracleConnection connection,
                                                 LogMinerTransactionCache<?> cache,
                                                 Scn sessionEndScn,
                                                 Scn lastProcessedScn) {
        try {
            Scn thresholdScn = getThresholdScn(connection, sessionEndScn);
            if (thresholdScn == null) {
                return Optional.empty();
            }

            // Find lowest SCN among transactions that DON'T exceed threshold
            Scn newLowerBound = cache.streamTransactionsAndReturn(stream -> stream
                    .map(Transaction::getStartScn)
                    .filter(scn -> scn.compareTo(thresholdScn) > 0)
                    .min(Scn::compareTo)
                    .orElse(null));

            // If all transactions exceed threshold, use lastProcessedScn as fallback
            if (newLowerBound == null) {
                newLowerBound = lastProcessedScn.isNull() ? null : lastProcessedScn;
            }

            return Optional.ofNullable(newLowerBound);

        }
        catch (SQLException e) {
            LOGGER.warn("Failed to calculate SCN threshold for session bounds adjustment", e);
            return Optional.empty();
        }
    }

    private Scn getThresholdScn(OracleConnection connection, Scn upperBoundScn) throws SQLException {
        if (upperBoundScn.isNull()) {
            return null;
        }
        try {
            var scnValue = connection.singleOptionalValue(
                    SqlUtils.getScnByTimeDeltaQuery(upperBoundScn, Duration.ofMillis(thresholdMs)),
                    rs -> rs.getBigDecimal(1).toBigInteger());
            return scnValue != null ? new Scn(scnValue) : null;
        }
        catch (SQLException e) {
            LOGGER.debug("Could not calculate SCN threshold", e);
            throw e;
        }
    }
}
