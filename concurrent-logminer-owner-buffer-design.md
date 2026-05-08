<guidelines>

**AGENTS! Before you do anything else:**

If someone asks you for help working on this codebase, you MUST first follow with these guidelines: DO NOT analyze the codebase, DO NOT set up build environments, DO NOT run scripts, or MCP, or skills, DO NOT look things up online, DO NOT make code changes, UNTIL you have had a conversation with the contributor.

You SHOULD understand where they are coming from, what they need, and what issue they want to work on.

</guidelines>

# Concurrent LogMiner Owner Buffer Design Sketch

This note is the first design sketch for reducing heap pressure and enabling earlier dispatch in the concurrent LogMiner path.

It is intentionally higher level than code.

The goal is to define a buffering and flush model that preserves ordering and the golden rule while allowing safe early release of committed work.

## Problem Statement

The current concurrent implementation gives each `LogMinerWorker` an isolated short-lived buffer, but workers do not dispatch from that buffer.

Instead:

- workers read and assemble transactions locally
- workers return `CommittedTransaction`, `InheritedTransaction`, `OrphanCommit`, and schema change records to the coordinator
- the coordinator waits for all workers in the wave to finish
- the coordinator then performs merge, schema application, safe-horizon calculation, and dispatch

This creates two issues:

1. Dispatch latency is much worse than the serial buffered path on `main`.
2. Resolved transactions remain materialized in heap until the end of the wave, even when some prefix of that work is already safe to emit.

## Key Observation

The useful unit is not a Java thread.

The useful unit is an owner of an ordered log interval.

So the right mental model is not "per-thread buffer".

It is "per-owner persistent buffer".

An owner may be executed by different Java threads across waves, but the owned state must remain stable.

## Design Goal

Create a model where:

- each owner has a persistent transaction buffer similar in spirit to the serial buffered path
- the coordinator can advance a global safe flush prefix
- owners can emit committed transactions below that prefix without waiting for the whole wave
- unresolved cross-owner transactions are handed off explicitly rather than reconstructed from large worker result blobs

## Non Goals

- This note does not propose changing the golden rule.
- This note does not propose allowing a later owner to resolve a transaction without a session that covers the transaction start.
- This note does not attempt to solve all schema replay details in full implementation detail.

## Working Terms

- `Owner`: the logical owner of a contiguous SCN slice or lane.
- `Owner Buffer`: the persistent transaction buffer for one owner.
- `Flush Prefix`: the highest commit-order prefix that is proven safe to emit now.
- `Handoff`: the transfer of unresolved transaction state from one owner to a later owner or continuation unit.
- `Barrier`: any unresolved condition that prevents later committed transactions from being emitted safely.

## Proposed Model

### 1. Persistent Owner Buffers

Each owner keeps a persistent buffer across waves.

That buffer contains at least:

- open transactions whose `START` is owned by that owner
- resolved transactions not yet flushed
- schema changes not yet applied or not yet flushed
- metadata describing the owner read frontier

This is closer to the existing serial buffered model than the current worker-result model.

### 2. Ownership Is By Ordered Partition, Not Thread Identity

The system partitions archive logs into ordered ownership lanes.

Examples:

- with 2 owners, owner 0 owns the earlier contiguous archive interval and owner 1 owns the later contiguous archive interval
- with N owners, each owner owns one ordered contiguous interval in the wave

An executor thread may process any owner, but the owner buffer is stable.

### 3. Resolved Work Stays In The Owner Buffer Until Safe

When an owner sees a commit for a transaction it can resolve authoritatively, that transaction becomes locally resolved in the owner buffer.

It is not immediately emitted unless the coordinator has already established that the transaction lies within the current safe flush prefix.

### 4. Coordinator Computes A Global Flush Prefix

The coordinator does not need to merge the entire wave before any dispatch.

It needs to answer a narrower question:

"What is the highest globally safe prefix of resolved work that can be emitted now without later violating commit order or schema order?"

That prefix is limited by barriers.

## Barriers That Block Early Flush

The flush prefix must stop before the earliest barrier.

The important barrier types are:

1. An unresolved transaction owned by an earlier owner.
2. A known cross-owner continuation whose final commit position has not yet been fully incorporated.
3. A schema change that must be applied before later DML but whose relative ordering is not yet settled.
4. Any transaction whose trusted prefix or continuation safety is not yet established.

This means early flush is possible only for the prefix strictly below the earliest unresolved ordering risk.

## Core Safety Intuition

The user intuition can be restated more formally:

If owner 0 has finished reading its interval, and every transaction that started in owner 0 and commits beyond owner 0 is recorded as unresolved handoff state, then owner 0 is authoritative over the flushable committed prefix below the first handoff barrier.

That is the key rule.

So owner 0 does not need to wait for the full wave.

It only needs to wait until the coordinator confirms where the earliest barrier is.

## Two Owner Example

Assume:

- owner 0 reads the earlier archive interval
- owner 1 reads the later archive interval

### Safe case

Owner 0 completes and has:

- resolved transactions with commits entirely inside owner 0 interval
- unresolved transactions whose starts are in owner 0 but whose commits are not yet seen in owner 0 interval

Owner 0 may flush:

- all resolved transactions whose commit ordering is strictly below the earliest unresolved cross-owner barrier

Owner 0 may not flush:

- any transaction at or above the first unresolved cross-owner barrier

Owner 1 may not flush its own resolved transactions if owner 0 still has unresolved cross-owner transactions that could commit before them.

That matches the intuition:

- earlier owner can release its safe prefix
- later owner waits behind unresolved earlier-owner continuations

## N Owner Generalization

For owners `0..N-1` ordered by SCN interval:

- owner `i` may flush only up to the minimum barrier produced by owners `0..i`
- owner `i+1` can never prove safety using only its own state
- barriers propagate to the right, never to the left

This scales because the coordinator only needs a compact per-owner summary:

- earliest unresolved continuation barrier
- earliest pending schema barrier
- lowest unflushed resolved commit
- current owner read frontier

The coordinator does not need to repeatedly rebuild one giant global list if it maintains these summaries incrementally.

## Buffer Contents Per Owner

An owner buffer should contain at least the following categories.

### Open transaction state

- transaction id
- start SCN
- trusted prefix SCN
- last read SCN
- accumulated events
- user and redo metadata

### Resolved but unflushed transactions

- commit SCN
- commit RS_ID
- ordered event list
- schema dependencies if needed

### Schema queue

- schema change SCN
- ordering metadata
- whether it is already applied internally
- whether it is already emitted externally

### Ownership state

- owner interval bounds
- latest completed read frontier
- earliest unresolved barrier
- latest flushed commit

## Handoff Model

The current code models handoff through `InheritedTransaction` and `OrphanCommit` objects returned from workers.

The proposed model keeps the same logical concepts but makes them first-class ownership state rather than temporary wave results.

Recommended direction:

- unresolved transactions stay attached to the owner that owns their `START`
- continuation work units read additional intervals but update the owning buffer
- known-commit bridge or replay work is treated as a continuation against the owning transaction state, not as an isolated temporary reconstruction when possible

This reduces heap churn from repeatedly materializing large cross-wave result objects.

## Serial Fallback Relationship

Serial fallback should be fixed first as a narrow win.

Reason:

- the current concurrent source serial fallback still uses worker-result then merge-and-dispatch behavior
- this means even pure serial fallback lost the commit-time streaming behavior of `main`

Phase 1 should therefore restore a true serial buffered pass for:

- full serial fallback
- redo follow-up after concurrent archive processing

That is the small change with immediate value.

## Phase 1 Concrete Plan

Phase 1 should not try to make the coordinator smarter.

It should remove the coordinator from the serial dispatch path.

The local hypothesis for Phase 1 is simple:

- if serial fallback and redo follow-up run through the existing buffered serial processor instead of `LogMinerWorkerCoordinator.executeSerial(...)`
- then commit-time dispatch behavior will return immediately for those paths
- and resolved transactions will stop accumulating until end-of-wave in the pure serial cases

The cheapest discriminating check is also simple:

- after the change, a serial fallback run should emit records as commits are processed rather than only after a worker result is merged

### Phase 1 Scope

Change only these two call sites in the concurrent source:

- full serial fallback over `allLogs`
- serial redo follow-up over `redoLogs`

Do not change concurrent archive-wave dispatch yet.

Do not change coordinator merge rules yet.

### Target Shape

The concurrent source should gain a dedicated serial execution path that reuses the buffered serial implementation model directly.

At a high level:

1. Concurrent archive work continues to run through the coordinator.
2. When the source needs serial fallback or redo follow-up, it invokes a buffered serial runner instead of `coordinator.executeSerial(...)`.
3. That buffered serial runner dispatches at commit time using the same mechanics as `BufferedLogMinerStreamingChangeEventSource.process(...)`.
4. Any unresolved transactions that remain after the serial pass are converted back into coordinator state for the next concurrent planning step.

### Implementation Options

There are two realistic implementation options.

#### Option A: Extract a reusable buffered serial runner

Create a reusable helper that owns the serial buffered pass for an explicit log set and read range.

Expected responsibilities:

- register the provided log set in the mining session
- start the session with the supplied session bounds
- execute the same query/processing loop used by the buffered source
- dispatch data and transaction events at commit time
- return the resulting processed SCN plus remaining unresolved transaction state

Expected inputs:

- explicit log list
- session start SCN
- read start SCN
- session end SCN
- injected inherited transactions to seed the transaction cache

Expected outputs:

- processed SCN
- unresolved transactions remaining after the serial pass
- any metadata needed to update coordinator pending state

This is the cleaner long-term shape.

#### Option B: Add a narrow serial adapter inside the concurrent source

If extraction is too large for the first patch, add a narrow adapter in the concurrent source that wraps the minimum buffered behavior needed for fallback.

This should still preserve the same core rule:

- no `WorkerResult`
- no wave-end merge for the serial path
- direct commit-time dispatch through buffered processing

Option B is acceptable only if it is clearly temporary and does not duplicate too much of the buffered source internals.

### Recommended Direction

Prefer Option A, but keep the first patch narrow.

The narrowest viable implementation is:

1. Extract a small reusable serial-buffered execution helper from `BufferedLogMinerStreamingChangeEventSource`.
2. Let `ConcurrentBufferedLogMinerStreamingChangeEventSource` call that helper for:
   - full serial fallback
   - redo follow-up
3. Leave `LogMinerWorkerCoordinator.executeSerial(...)` in place temporarily only if tests still need it for isolated coordinator behavior.

### State Bridging Required For Phase 1

The key technical hurdle is not dispatch.

It is state transfer between coordinator-held inherited transactions and the buffered serial transaction cache.

Phase 1 therefore needs one explicit bridge in each direction.

#### Coordinator to buffered serial seed

Before the serial pass begins, seed the buffered transaction cache with the equivalent of current `InheritedTransaction` state:

- transaction id
- start SCN
- start time
- username and client id
- redo thread id
- trusted prefix SCN if needed by the current branch model
- last read SCN
- accumulated event list

This is the minimum requirement to let the buffered serial pass continue an open transaction instead of rediscovering it from scratch.

#### Buffered serial back to coordinator pending state

After the serial pass completes:

- any still-open transactions must be converted back into coordinator pending inherited state
- any matching pending orphan commit state that is now resolved must be removed
- processed SCN must become the source's next `currentReadScn`

For Phase 1, it is acceptable if this conversion remains explicit and slightly mechanical.

### Suggested Code Boundaries

The likely owning abstraction is near the existing buffered source, not in the coordinator.

Concrete boundaries to aim for:

- keep `ConcurrentBufferedLogMinerStreamingChangeEventSource` responsible for deciding when serial fallback is needed
- keep `LogMinerWorkerCoordinator` responsible for concurrent planning and pending cross-wave state
- move serial commit-time execution into a reusable buffered-path helper or extracted component

That keeps the coordinator from becoming a second serial processor.

### Minimal API Sketch

This is only a sketch, not a required final signature.

```java
SerialPassResult executeBufferedSerialPass(
   List<LogFile> logs,
   Scn sessionStartScn,
   Scn readStartScn,
   Scn sessionEndScn,
   List<InheritedTransaction> inheritedTransactions)
```

Where `SerialPassResult` contains at least:

- `processedScn`
- `remainingInheritedTransactions`

Optional fields if needed:

- `resolvedTransactionIds`
- `appliedSchemaScns`
- `lastReadScn`

### What Must Not Happen In Phase 1

Phase 1 should not:

- introduce owner buffers yet
- modify concurrent worker result merging
- attempt partial early flush for concurrent archive owners
- rewrite bridge or replay planning

If Phase 1 starts changing those areas, scope has drifted.

### Acceptance Criteria

Phase 1 is successful if all of the following are true.

1. Full serial fallback no longer calls the coordinator worker-result serial path for dispatch.
2. Redo follow-up no longer waits for wave-end merge to emit committed transactions.
3. Serial fallback dispatch timing matches the buffered serial model closely enough that records can appear before the entire serial pass ends.
4. Cross-wave inherited transactions still survive a fallback pass correctly.
5. Existing concurrent archive-wave behavior is unchanged.

### Focused Validation For Phase 1

The best validation sequence is:

1. A focused test or trace showing serial fallback emits records before end-of-wave completion.
2. A redo-follow-up scenario where inherited archive transactions resolve during the serial redo pass and dispatch immediately.
3. A narrow Oracle connector compile or targeted test run for the touched slice.

If runtime tracing is easier than a new test for the first iteration, that is acceptable for the design step, but the implementation should still aim for a regression test.

### Why Phase 1 Is Worth Doing First

Phase 1 gives an immediate win without forcing the owner-buffer design to be solved prematurely.

It restores the already-proven serial dispatch behavior in the places where concurrency is not buying anything anyway.

That reduces latency and heap pressure in the simplest paths, while leaving the harder concurrent early-flush design for a second step.

## Step 2 Concrete Plan

Step 2 should extend the same dispatch idea into concurrent archive waves, but still avoid the full owner-buffer rewrite.

The narrow hypothesis for Step 2 is:

- if the coordinator processes worker results in completion order
- but only merges and dispatches the longest contiguous worker prefix whose earlier owners are already complete
- then worker 0 and any later completed contiguous owners can flush their safe prefix during the archive wave without waiting for the final worker result

This is intentionally smaller than the long-term owner-buffer model.

It keeps the current worker result representation.

It changes only when the coordinator is allowed to merge and dispatch those results.

### Step 2 Scope

Change only archive-wave execution inside `LogMinerWorkerCoordinator.executeWave(...)`.

Do not change:

- serial fallback behavior added in Phase 1
- BRIDGE execution model
- replay execution model
- cross-wave state representation

### Step 2 Mechanics

The coordinator should:

1. Submit all archive-wave workers concurrently.
2. Collect worker results as each worker completes.
3. Buffer out-of-order completions by `workerId`.
4. Whenever the next expected `workerId` is available, merge and dispatch that contiguous ready prefix immediately.
5. Carry forward unresolved state, safe-horizon holdbacks, and schema holdbacks between prefix merges.
6. After all workers complete, run replay planning using the full wave result set.

### Why The Prefix Rule Is Safe

Later worker results cannot reduce the safety of a prefix that is already below the earliest unresolved barrier from the completed earlier owners.

For an unresolved transaction in owner `i` whose commit location is still unknown, the safe barrier remains `lastReadScn` until a later owner reveals the orphan commit.

That may be conservative, but it is safe.

When the later owner result arrives, the coordinator may widen the flushable prefix, but it does not need to retract any already-flushed work.

### Required Internal State

Step 2 needs only two new pieces of wave-local state:

1. A map of completed worker results waiting for earlier owners.
2. A cumulative list of schema changes already applied in the current wave.

The schema history is necessary because DDL contamination detection for a later worker must still see DDLs that were already applied and removed from `pendingSchemaChanges` by an earlier prefix merge.

### Manageable Implementation Steps

#### Step 2.1: Completion-ordered worker collection

Add a completion-ordered worker execution path for archive waves.

Expected result:

- worker results are no longer forced through submission order
- test infrastructure can inject completion order directly

#### Step 2.2: Contiguous prefix merge loop

Inside `executeWave(...)`, hold completed results by `workerId` and repeatedly merge the longest contiguous ready prefix.

Expected result:

- if worker 0 completes first, its safe prefix can dispatch immediately
- if worker 1 completes before worker 0, it waits in the buffer until worker 0 arrives

#### Step 2.3: Incremental merge state

Refactor merge logic so it can be called repeatedly with only the newly available prefix results while still reusing:

- `pendingDispatch`
- `pendingSchemaChanges`
- `pendingInheritedTransactions`
- `pendingOrphanCommits`

Expected result:

- the coordinator behaves like a rolling merge rather than a one-shot wave-end merge

#### Step 2.4: Preserve DDL contamination detection across prefixes

Track schema changes already applied in the current wave and include them when checking later prefix transactions for stale-schema contamination.

Expected result:

- early prefix dispatch does not accidentally bypass targeted DDL replay

#### Step 2.5: Final replay planning after full wave completion

Keep replay unit planning at full-wave completion, using the complete set of wave results.

Expected result:

- overlapping worker tails are still replayed correctly
- Step 2 does not need to invent prefix-local replay scheduling

### Step 2 Acceptance Criteria

Step 2 is successful if all of the following are true.

1. Archive-wave worker results are processed in completion order.
2. The coordinator dispatches completed contiguous worker prefixes before the final worker result is required.
3. Out-of-order worker completion does not break owner ordering.
4. Safe-horizon holdback behavior remains correct across incremental prefix merges.
5. DDL-contamination replay still works when a DDL is applied in one prefix and affected DML appears in a later prefix.

### Step 2 Focused Validation

The best narrow validation set is:

1. A test showing worker completion order `[0, 1]` yields two incremental prefix merges.
2. A test showing worker completion order `[1, 0]` yields one merged prefix once owner 0 arrives.
3. A test showing an earlier prefix DDL still quarantines contaminated transactions from a later prefix.
4. A focused Oracle connector compile or targeted test run for the touched slice.

### What Step 2 Still Does Not Do

Step 2 does not create persistent owner buffers yet.

It is still a worker-result design.

The improvement is that the coordinator stops waiting for the whole archive wave when an earlier owner prefix is already safe to emit.

## Concurrent Early Flush Relationship

After serial fallback is corrected, concurrent early flush can build on the owner-buffer model.

The main implementation question is not "can worker 0 emit?"

It is:

"What compact barrier summary is sufficient for the coordinator to authorize owner 0 to emit a prefix without seeing all later resolved transactions?"

That is the core design question for step 2.

## Suggested Flush Algorithm Shape

This is not final code design, but a target shape.

1. Each owner finishes a read slice and publishes a summary.
2. The summary contains:
   - earliest unresolved barrier
   - earliest pending schema barrier
   - lowest resolved unflushed commit
   - latest completed read frontier
3. The coordinator computes the global safe prefix.
4. The coordinator authorizes each owner to flush only the portion of its resolved queue below that prefix.
5. Unflushed remainder stays in the owner buffer for the next iteration.

This preserves ordering while allowing earlier owners to release safe work before later owners complete all continuation logic.

## Why This Helps Heap

Compared with the current whole-wave merge model, owner buffers reduce heap pressure in two ways.

1. Safe committed prefixes can be flushed and discarded earlier.
2. Cross-wave unresolved state does not need to be repeatedly expanded into large one-wave result objects just to be stitched back together.

Heap usage becomes more proportional to:

- unresolved transactions
- not-yet-safe resolved prefixes
- pending schema barriers

rather than proportional to the full resolved output of every completed owner in the wave.

## Main Risks

1. Ownership drift

If ownership is tied to executor threads instead of stable partitions, state management will become fragile.

2. Schema ordering

DDL and DML ordering must remain globally correct across owners.

3. Cross-owner continuation complexity

If continuation updates both the source owner and the continuation executor state without a clear single owner of truth, correctness will degrade quickly.

4. Flush prefix mistakes

An incorrect barrier summary will produce either reordering or duplicate replay work.

## Recommended Phased Plan

### Phase 1

Restore true serial buffered dispatch behavior for:

- serial fallback
- redo follow-up

No concurrent early flush yet.

### Phase 2

Introduce the owner-buffer abstraction and coordinator barrier summaries.

Still keep end-of-wave flush if needed.

### Phase 3

Enable prefix flush for the earliest owner once the coordinator can prove the safe prefix.

### Phase 4

Extend prefix flush across N owners with explicit schema barriers and continuation ownership rules.

## Open Questions

1. Should ownership persist across waves by fixed lane index, by first log in interval, or by another stable identifier?
2. Should continuation work update the original owner buffer directly, or produce a delta that the coordinator applies to the original owner?
3. How should schema changes be represented so that schema barriers are cheap to compute?
4. Do we want one owner buffer implementation reused from the existing buffered cache abstractions, or a lighter concurrent-specific owner state store?

## Current Recommendation

The best near-term direction is:

- fix serial fallback first
- do not build around Java thread-local buffers
- design concurrent early flush around persistent owner buffers and coordinator-managed safe prefixes

That keeps the model close to the serial buffered design on `main`, while still respecting the continuation and ordering constraints unique to concurrent mining.