# Concurrent Oracle LogMiner High-Level Summary

This note summarizes the current multithreaded Oracle LogMiner work on the `multi-threading` branch at a high level.

It is intentionally written as a behavior and architecture overview rather than a code walkthrough.

## What the change does

The new implementation allows Debezium's Oracle LogMiner reader to process archive logs concurrently when that is safe, while still preserving global transaction commit order.

At a high level the flow is:

1. Split the current log set into archive logs and online redo logs.
2. Read archive logs concurrently when there are enough archive logs to justify it and there are no unresolved unknown-commit transactions.
3. Let the coordinator merge worker results in order and decide what is safe to dispatch.
4. If a transaction was split across workers, resolve it with a later bridge pass once both sides are known.
5. If online redo logs are present, process them in a serial follow-up pass.
6. If the system cannot safely continue concurrently, fall back to serial processing until the unsafe condition is gone.

The design tries to get parallel read throughput without breaking the Oracle LogMiner "golden rule" or emitting transactions out of commit order.

## Main roles

## Workers

Workers are the units that actually query `V$LOGMNR_CONTENTS` for a specific read window.

Each worker:

- opens its own LogMiner session
- reads one planned `WorkUnit`
- rebuilds any inherited open transactions into a local cache before reading
- parses DML and DDL records in its range
- turns complete transactions into resolved committed transactions
- carries still-open transactions forward as inherited transactions
- records orphan commits when it sees a commit without having seen the corresponding start in its own owned window

Workers are separate threads when running in concurrent mode. They come from the coordinator's executor pool.

In serial mode, the same `LogMinerWorker` implementation is reused, but it is invoked directly on the calling thread instead of being submitted to the pool.

## Bridge

The bridge is not a dedicated long-running thread.

It is a special kind of planned work unit.

The bridge exists to resolve cross-log or cross-worker transactions where:

- one worker already found the transaction start and buffered its events
- another worker found the commit and reported it as an orphan commit

Once the coordinator has both pieces, it can plan a `BRIDGE` unit.

That bridge unit:

- preloads the inherited transaction state into a worker-local cache
- rereads only the unresolved unsafe interval
- resolves the transaction when the commit is encountered again in a context that now has the full transaction state available

Bridge work runs on a worker-pool thread just like a normal worker unit. It is separate as a logical role, not as a permanent thread type.

Compatible bridge pairs can be batched so one bridge pass resolves multiple split transactions in one read.

## Coordinator

The coordinator is the control point.

It is not a separate background worker thread. It runs on the main streaming control path and is responsible for orchestration.

The coordinator:

- decides whether concurrent reading is allowed for the current wave
- partitions logs into worker work units
- tracks cross-wave state such as inherited transactions and orphan commits
- merges worker results in worker-prefix order
- computes what is safe to dispatch now and what must be held back
- plans bridge follow-up work for known split transactions
- plans replay work when a worker's unsafe tail must be re-mined
- runs serial continuation when concurrency is no longer safe

The key rule is that the coordinator, not the workers, owns global ordering and dispatch safety.

## Are these separate threads?

- Workers: yes, in concurrent mode they run as separate executor threads.
- Bridge: no dedicated thread type. A bridge is a special work unit executed by one of the same worker-pool threads.
- Coordinator: no separate worker thread. It runs on the calling streaming thread and orchestrates the wave.
- Serial fallback: no worker pool concurrency. One worker instance runs directly on the calling thread.

## Why the bridge exists

Concurrent reads divide the archive log range into owned slices.

That creates a common split-transaction case:

- an earlier worker sees `START` and some DML
- a later worker sees the `COMMIT`

The later worker cannot safely resolve that transaction because it does not own the transaction history. So it emits an orphan commit marker instead.

The coordinator then matches:

- inherited transaction state from the earlier worker
- orphan commit state from the later worker

That pair becomes a bridge candidate.

Without this extra phase, the system would either lose the transaction context or be forced to serialize much more of the normal archive-log path.

## How dispatch stays ordered

The coordinator can hold transactions back when there is unresolved earlier work.

The important idea is a safe dispatch horizon.

If there is an unresolved transaction that could still commit before a later resolved transaction, the later transaction is not dispatched yet. Instead it is held in pending state until the earlier unresolved work is resolved.

This is what lets the implementation parallelize reads while still preserving global commit order.

## Example 1: Two readers, bridge required, bridge resolves everything

This is the simple happy-path bridge example.

Assume:

- there are two archive logs in the wave: `log1` and `log2`
- there are two workers
- three transactions start in `log1` and commit in `log2`
- there are no online redo logs in scope

### Wave 1

On the calling streaming thread, the coordinator plans two normal worker units.

- Worker 0 reads `log1`
- Worker 1 reads `log2`

Those two worker units are then executed on worker-pool threads, not on the calling thread.

Worker 0 sees:

- the `START` records for transactions `T1`, `T2`, and `T3`
- their DML rows
- no commits yet

Worker 0 finishes by returning three inherited transactions.

Worker 1 sees:

- the `COMMIT` rows for `T1`, `T2`, and `T3`
- no starts for those transactions in its own read window

Worker 1 therefore cannot resolve them directly and returns three orphan commits.

At the end of the wave, the coordinator now knows:

- the full buffered history for `T1`, `T2`, and `T3`
- the exact commit locations for `T1`, `T2`, and `T3`

That merge and planning step is done by the coordinator on the calling thread.

Those matched pairs are enough to plan a bridge.

### Bridge follow-up

In the next pass, the coordinator creates one batched `BRIDGE` work unit covering all three transactions.

That planning decision is made on the calling thread by the coordinator.

That bridge unit is executed by one worker-pool thread.

The bridge:

- preloads `T1`, `T2`, and `T3` into the worker cache
- rereads the unread unsafe interval in `log2`
- encounters each commit again
- resolves each transaction as a full committed transaction

Because all the split transactions are now resolved, the coordinator can dispatch them in commit order.

That final merge and dispatch decision is again performed by the coordinator on the calling thread after the bridge worker finishes.

This is the ideal bridge case:

- the first concurrent wave discovers both halves of the split work
- the bridge pass resolves all of it
- no serial fallback is required

## Example 2: Serialized fallback and when it comes into play

The serialized fallback is used when the system cannot safely continue concurrent ownership.

The most important case is an inherited transaction whose commit location is still unknown.

Assume:

- wave 1 reads `log1` and `log2`
- a transaction `T1` starts in `log1`
- `T1` is still open at the end of the wave
- no worker has seen the commit yet

After that wave, the coordinator has, on the calling thread:

- one inherited transaction for `T1`
- no matching orphan commit

That means the commit location for `T1` is unknown.

At this point the system does not know which later concurrent worker should own continuation of `T1`, because there is no precise commit boundary to bridge toward yet.

So in the next wave, instead of running more concurrent archive workers, the coordinator forces serial processing.

That fallback decision is made on the calling thread by the coordinator.

### What serial fallback does

The coordinator creates a single work unit covering the whole active log set and injects all pending inherited transactions into that one worker.

Unlike concurrent mode, that worker is not submitted to the worker pool. It is executed directly on the calling thread.

That single worker then:

- continues reading forward alone
- keeps ownership of `T1`
- accumulates any additional DML for `T1`
- eventually finds the commit if it appears in a later log
- resolves and dispatches it directly

This matters because one reader now owns the full continuation path. There is no ambiguity about which worker should carry the open transaction forward.

So in serial fallback, both orchestration and actual mining execution are effectively happening on the calling thread.

### When it stops falling back

Once the unknown-commit transaction is resolved, or once the state becomes precise enough to bridge safely, the concurrent archive path can be used again in later waves.

## Online redo log serial follow-up

There is a second serial case that is separate from the unknown-commit fallback.

Even when archive logs are read concurrently, online redo logs are still processed serially afterward.

That happens because online redo logs are still being written, so the design does not allow them to be mined concurrently with the archive worker pool.

In that case, the concurrent archive phase still uses worker-pool threads, but the redo follow-up is run directly on the calling thread through the serial path.

So there are really two serial behaviors:

1. Full serial fallback when concurrency is unsafe.
2. Serial redo follow-up after a successful concurrent archive phase.

## Practical mental model

The simplest way to think about the implementation is:

- workers read slices
- the coordinator decides what those slices mean globally
- the bridge repairs transactions that were split across slices
- serial fallback takes over when one owner must continue alone

That is the core behavior of the current design.