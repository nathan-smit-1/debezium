# Concurrent LogMiner Two-Wave Scenario

This note describes the behavior of the new concurrent LogMiner archive-log feature using only the latest commit's implementation logic. It does not rely on tests.

## Assumptions

- `log.mining.concurrent.readers > 1`, so the concurrent source is selected.
- Only archive logs are in scope, so concurrent reading is allowed.
- Wave 1 sees `log1` and `log2`.
- Wave 2 sees `log2` and `log3`.
- There are exactly two worker threads configured.
- Scenario:
  - Three transactions start in `log1` and commit in `log2`.
  - One transaction starts in `log2` and commits in `log3`.

## Key Objects

- `ConcurrentBufferedLogMinerStreamingChangeEventSource`
  - Runs the outer batch loop and calls the coordinator once per wave.
- `LogMinerWorkerCoordinator`
  - Plans work units for the wave.
  - Tracks cross-wave state.
  - Merges and dispatches resolved transactions in commit SCN order.
- `LogMinerWorker`
  - Executes one work unit.
  - Returns resolved transactions, unresolved inherited transactions, orphan commits, and schema changes.
- `InheritedTransaction`
  - An open transaction carried from a prior wave.
- `OrphanCommit`
  - A commit seen without a matching start in that worker's local read window.
- `BRIDGE` work unit
  - Resolves a transaction when the coordinator already knows both the inherited transaction and its commit SCN.
- `SEARCH` work unit
  - Continues looking for the commit of an inherited transaction when the commit SCN is still unknown.

## State Before Wave 1

- No pending inherited transactions.
- No pending orphan commits.
- No pending dispatch backlog.

## Wave 1

### Planning

The outer loop collects the log set for the current read SCN and passes it to the coordinator.

Because there are two archive logs and two readers, the coordinator creates two normal `WORKER` units:

- Worker thread 1 gets `log1`.
- Worker thread 2 gets `log2`.

There are no `BRIDGE` or `SEARCH` units yet, because there is no pending cross-wave state before the first wave.

### Worker Thread 1: reads `log1`

Thread 1 sees the three transactions start in `log1`.

For each of those three transactions:

- It processes the `START` row.
- It accumulates any DML rows into its private worker-local cache.
- It does not see the `COMMIT`, because the commit is in `log2`.

At the end of the worker run:

- All three transactions are still open.
- The worker returns them as `InheritedTransaction` objects.
- Each inherited transaction records:
  - its original `startScn`
  - accumulated events so far
  - `lastReadScn = log1.nextScn`

Meaning: thread 1 says, "I know these transactions started earlier and I have partial contents, but they are unresolved."

### Worker Thread 2: reads `log2`

Thread 2 sees the three commits for the transactions that started in `log1`.

But thread 2 did not see their starts in its own read range and inherited nothing in wave 1, so when `handleCommit` runs:

- it cannot find those transactions in its local cache
- it creates three `OrphanCommit` records instead of resolved transactions

Thread 2 also sees the separate transaction that starts in `log2` and commits only in `log3`.

For that transaction:

- it sees the `START`
- it accumulates its DML
- it does not see the `COMMIT`
- it returns that transaction as another `InheritedTransaction`

Meaning: thread 2 says, "I found three commits with no local start, and one new open transaction that continues beyond my log."

### Coordinator After Both Threads Finish

The coordinator merges worker results and updates cross-wave state.

Pending state after wave 1 becomes:

- `pendingInheritedTransactions`
  - three transactions from `log1 -> log2`
  - one transaction from `log2 -> log3`
- `pendingOrphanCommits`
  - three orphan commits from `log2` matching the first three transactions
- `pendingDispatch`
  - possibly some resolved transactions from wave 1 held back by the safe horizon

### Safe Dispatch Horizon in Wave 1

The coordinator computes a safe dispatch horizon so it does not emit later transactions ahead of unresolved cross-log transactions.

For this scenario, horizon candidates are:

- for the three `log1 -> log2` transactions: their known orphan `commitScn` values in `log2`
- for the `log2 -> log3` transaction: its `lastReadScn`, because its commit is still unknown

The coordinator picks the minimum of those candidates.

Effect:

- any resolved transaction with `commitScn >= safeHorizon` is not dispatched yet
- it is moved into `pendingDispatch`
- this preserves global commit order until the cross-log transactions are resolved

### Summary of Wave 1

- Thread 1 produces three inherited transactions.
- Thread 2 produces three orphan commits and one inherited transaction.
- No bridge runs yet.
- The coordinator now has enough information to schedule bridges in the next wave.

## Wave 2

### Planning

The next outer-loop iteration runs from the updated read SCN.

Work units are planned in this order:

1. `BRIDGE` units
2. `SEARCH` units
3. ordinary `WORKER` units for any remaining uncovered logs

### Bridge Behavior for the Three `log1 -> log2` Transactions

The coordinator matches each orphan commit from wave 1 with its corresponding inherited transaction.

When those matches share the same unread window, they are batched into a single `BRIDGE` unit so they can be resolved in one pass.

In this scenario, the three `log1 -> log2` transactions are compatible for batching, so the coordinator can create one shared bridge pass with:

- `sessionStartScn = earliest unresolved startScn in log1 - 1`
- `sessionEndScn = latest commitScn in log2`
- `readStartScn = shared lastReadScn from the end of log1`
- `readEndScn = latest commitScn in log2`
- `inheritedTransactions = all three carried-forward transactions`

This is what the bridge actually does:

- it is not a permanent third thread
- it is a special work unit executed by one of the same worker-pool threads
- it preloads all three inherited transactions into the worker-local cache
- it scans the unread portion of `log2` once and resolves whichever of those transactions commit in that window

When the bridge worker reaches each commit row in `log2`:

- the transaction is already present in the worker cache from the inherited preload
- `handleCommit` resolves it as a full `CommittedTransaction`
- it is no longer treated as an orphan

Meaning: the bridge can turn all three `log1 -> log2` split transactions into normal resolved committed transactions in a single pass.

### Search Behavior for the `log2 -> log3` Transaction

The `log2 -> log3` transaction still has no known commit after wave 1, so the coordinator builds a `SEARCH` unit for it.

That search unit:

- preloads the inherited transaction from `log2`
- starts reading at `lastReadScn = log2.nextScn`
- scans the first unread log, which is `log3` in this scenario

If the commit is present in `log3`:

- the search unit resolves the transaction in wave 2

If the commit is not yet present:

- the search unit returns an updated `InheritedTransaction`
- `lastReadScn` moves forward again
- the transaction remains pending for another wave

### What Each Thread Does in Wave 2

The exact thread-to-unit assignment is up to the executor, but logically with two worker threads the work looks like this:

- One worker-pool thread executes the single batched bridge unit for the `log1 -> log2` transactions.
- The other worker-pool thread executes the search unit for the `log2 -> log3` transaction, or the normal worker unit for any later uncovered log.

What matters is not the numeric thread identity but the unit type:

- the batched bridge unit resolves the three `log1 -> log2` transactions in one pass
- the search unit looks for the `log2 -> log3` commit in `log3`

If all logs involved in bridge/search are covered by those units, there may be no ordinary `WORKER` units left in wave 2.

### Coordinator After Wave 2

The coordinator prepends any previously held-back `pendingDispatch`, adds the newly resolved bridge results, adds the search result if it resolved, sorts everything by `commitScn`, and dispatches anything now below the new safe horizon.

There are two outcomes:

#### Outcome A: the `log2 -> log3` transaction commits in `log3` and is found in wave 2

- all four cross-log transactions are now resolved
- the safe horizon may disappear entirely
- previously held-back transactions in `pendingDispatch` can now be emitted
- the three bridged transactions and the `log2 -> log3` transaction are dispatched in correct global commit order

#### Outcome B: the `log2 -> log3` transaction still does not commit within wave 2

- the three bridged transactions are resolved
- the `log2 -> log3` transaction remains in `pendingInheritedTransactions`
- the safe horizon remains active
- some resolved transactions may still be held back for a later wave

## Compact State Table

| Moment | Thread / Unit | Reads | Result |
|---|---|---|---|
| Wave 1 | Worker 1 | `log1` | 3 inherited transactions |
| Wave 1 | Worker 2 | `log2` | 3 orphan commits, 1 inherited transaction |
| After Wave 1 | Coordinator | merge | pending inherited = 4, pending orphan commits = 3 |
| Wave 2 | Batched bridge unit | `log1 -> log2` window, read resumes after `log1` | resolves transactions 1, 2, and 3 in one pass |
| Wave 2 | Search unit | resumes after `log2`, scans `log3` | resolves or re-inherits transaction 4 |
| After Wave 2 | Coordinator | merge + dispatch | dispatches resolved work below any remaining horizon |

## Practical Summary

On the first loop, the feature intentionally allows the two worker threads to discover only half of the story:

- one side finds starts and open transactions
- the other side finds commits without starts

It does not try to force immediate resolution inside that same wave.

On the next loop, the coordinator uses the saved cross-wave state to create special follow-up work:

- `BRIDGE` units for transactions where both halves are now known
- `SEARCH` units for transactions where only the start side is known

That is the core design of the feature: discover split transactions in one wave, resolve them in a later wave without breaking global commit order.

## Code Anchors

- Concurrent source selection: `debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/logminer/buffered/BufferedLogMinerAdapter.java`
- Outer concurrent loop: `debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/logminer/concurrent/ConcurrentBufferedLogMinerStreamingChangeEventSource.java`
- Wave execution and planning: `debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/logminer/concurrent/LogMinerWorkerCoordinator.java`
- Worker behavior and commit/orphan handling: `debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/logminer/concurrent/LogMinerWorker.java`
- Shared log collection: `debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/logminer/AbstractLogMinerStreamingChangeEventSource.java`
- Log selection by read SCN: `debezium-connector-oracle/src/main/java/io/debezium/connector/oracle/logminer/LogFileCollector.java`

## Caveat From the Current Code

One implementation detail to keep in mind:

- each wave's available logs are collected from the current read SCN, not from the oldest inherited transaction start SCN

So the bridge mechanism is clearly planned by the coordinator, but whether an older file such as `log1` is still present in the next wave's collected log set depends on what the log collector returns for the advanced read position.

## Compact State Table: One Transaction Spans `log1 -> log5`

Scenario assumptions:

- All transactions resolve in the same log where they start except one.
- One transaction starts in `log1` and commits in `log5`.
- Intermediate logs `log2`, `log3`, and `log4` do not contain the commit for that long-running transaction.
- Other transactions in each wave resolve locally in their own worker slices.

| Wave | Carry-forward transaction state | Worker handling the carried transaction | Other worker activity | Coordinator state after merge |
|---|---|---|---|---|
| 1 | Transaction starts in `log1`, does not commit | Worker on `log1` returns it as one `InheritedTransaction` with `lastReadScn = log1.nextScn` | Worker on later log slices resolves normal in-log transactions | `pendingInheritedTransactions = 1`, no matching orphan commit yet |
| 2 | Still open after `log2` | One worker runs the carry-forward unit with session covering from the original `log1` start, read window resuming after `log1`, effectively scanning `log2` | Other worker reads the next uncovered log slice | Transaction is re-inherited with `lastReadScn = log2.nextScn`; safe horizon may hold back later commits |
| 3 | Still open after `log3` | One worker runs the carry-forward unit again, session still anchored to the original start, read window now scanning `log3` | Other worker reads the next uncovered log slice | Transaction is re-inherited with `lastReadScn = log3.nextScn` |
| 4 | Still open after `log4` | One worker runs the carry-forward unit again, session anchored to `log1`, read window now scanning `log4` | Other worker reads the next uncovered log slice | Transaction is re-inherited with `lastReadScn = log4.nextScn` |
| 5 | Commit appears in `log5` | One worker runs the carry-forward unit, session still covers the original start, read window scans `log5` and reaches the commit | Other worker reads any uncovered later log slice if present | Transaction resolves as a `CommittedTransaction`; pending inherited entry is cleared |
| After wave 5 | Fully resolved | No more carry-forward unit needed for this transaction | Normal worker partitioning resumes | Safe horizon can move forward or disappear if nothing else is unresolved |

### Short Read Of The Table

- There is only one carried transaction through waves 1 to 5.
- The worker assigned to that carried transaction keeps the original transaction context alive by preloading the inherited state each wave.
- The session remains anchored far enough back to satisfy LogMiner's start requirements.
- The read window advances one unread log at a time until `log5` contains the commit.
- Other workers can continue processing later uncovered logs in parallel.

## Compact State Table: Commit In Early `log2` Was Missed In Wave 1

Scenario assumptions:

- A transaction starts in `log1`.
- It actually commits in the early part of `log2`.
- The worker reading `log2` in wave 1 fails to observe that commit because of the low-watermark edge case.
- `log2` is still available when the next wave collects logs.

| Wave | What worker on `log1` sees | What worker on `log2` sees | Coordinator state after merge | What happens next |
|---|---|---|---|---|
| 1 | Sees `START` and DML, returns one `InheritedTransaction` with `lastReadScn = log1.nextScn` | Does **not** see the commit, so it produces neither a resolved transaction nor an orphan commit for that transaction | `pendingInheritedTransactions = 1`, `pendingOrphanCommits = 0` | Transaction is treated as still open |
| 2 planning | The inherited transaction has no known commit SCN | No orphan is available to build a bridge | Coordinator builds a `SEARCH` carry-forward unit targeting the first unread log after `lastReadScn`; here that is still `log2` | The retry session is anchored low enough to include the original start |
| 2 execution | Carry-forward worker preloads the inherited transaction from wave 1 | The same worker scans `log2` again, but now with `sessionStartScn = startScn(log1) - 1`, `readStartScn = log1.nextScn`, `readEndScn = log2.nextScn` | If the missed commit row is present in `scn > log1.nextScn && scn <= log2.nextScn`, the worker resolves it as a normal `CommittedTransaction` | The missed commit is recovered |
| After wave 2 | No inherited transaction remains for that transaction | No orphan commit remains either | Pending state for that transaction is cleared | Dispatch can proceed subject to the safe horizon for any other unresolved work |

### Will The Current Implementation Handle This?

Yes, in the normal case it should.

Why:

- Wave 1 leaves the transaction in `pendingInheritedTransactions` because the `log1` worker saw the start and the `log2` worker did not see the commit.
- The next wave does not need an orphan commit to retry; it will create a `SEARCH` unit because the transaction is still unresolved.
- That `SEARCH` unit will target `log2` again if `log2.firstScn >= lastReadScn` and `log2` is still returned by log collection.
- The session start for that retry is lowered back to the original transaction start, which is the exact safeguard Debezium normally relies on for long-running transactions.

So the implementation effectively says: if a commit was missed because the original `log2` worker's session started too high, the carry-forward retry can re-read the unread portion of `log2` with a lower session start and recover it.

### Conditions And Limits

This recovery depends on a few things:

- `log2` must still be available in the next wave's collected log set.
- The missed commit must fall within the retry query window: `scn > lastReadScn` and `scn <= log2.nextScn`.
- If the commit were somehow exactly equal to `lastReadScn`, the exclusive lower bound would skip it.

So the answer is not "always no matter what", but for the edge case you described, the current implementation is designed in a way that should recover it on the next wave.