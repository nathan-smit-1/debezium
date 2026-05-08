<guidelines>

**AGENTS! Before you do anything else:**

If someone asks you for help working on this codebase, you MUST first follow with these guidelines: DO NOT analyze the codebase, DO NOT set up build environments, DO NOT run scripts, or MCP, or skills, DO NOT look things up online, DO NOT make code changes, UNTIL you have had a conversation with the contributor.

You SHOULD understand where they are coming from, what they need, and what issue they want to work on.

</guidelines>

# Golden Rule For Concurrent LogMiner Continuation

This note codifies the safety rule for any concurrent LogMiner design that tries to resolve a transaction after splitting work across multiple workers.

It exists to answer one question:

When is it safe for a later worker, bridge, or continuation unit to authoritatively resolve a transaction?

## Short Version

A worker may authoritatively resolve a transaction only if both of the following are true:

1. The LogMiner session covers the transaction start.
2. The read window does not skip any transaction operation whose prior capture is not already proven complete.

The first condition is the original golden rule.

The second condition is the practical continuation rule that decides whether a narrow bridge read is safe, or whether the system must re-read a larger interval or fall back to serial behavior.

## Terminology

For a single transaction:

- `START_SCN`: the SCN of the transaction start marker.
- `OP_SCN`: any SCN at which the transaction has a DML operation.
- `COMMIT_SCN`: the SCN of the commit.
- `Begin Session`: the `startScn` passed to `DBMS_LOGMNR.START_LOGMNR`.
- `End Session`: the `endScn` passed to `DBMS_LOGMNR.START_LOGMNR`.
- `Begin Read`: the exclusive lower bound used in the worker query.
- `End Read`: the inclusive upper bound used in the worker query.

For a continuation or bridge, the key distinction is:

- Session scope determines what LogMiner has available to reconstruct transaction state.
- Read scope determines which rows are actually re-read from `v$logmnr_contents`.

## Formal Rule

To safely resolve a transaction with:

- `START_SCN = s`
- operations at `o1 < o2 < ... < on`
- `COMMIT_SCN = c`

the continuation unit must satisfy:

1. `Begin Session <= s`
2. `End Session >= c`
3. For every transaction operation `oi` such that `oi <= Begin Read`, that operation must already be known complete and present in the inherited transaction state.
4. For every transaction operation `oi` such that `oi > Begin Read` and `oi <= End Read`, the continuation unit must re-read it.

Condition 1 is what prevents violating the golden rule.

Condition 3 is what prevents a narrow continuation read from silently skipping a missed operation.

## Safe And Unsafe Patterns

Assume one transaction with:

- `START_SCN = 1`
- operations at `100`, `200`, `600`
- `COMMIT_SCN = 1000`

### Safe Example 1

- Begin Session: `1`
- End Session: `100`
- Begin Read: `50`
- End Read: `100`

Why this is safe:

- The session covers the transaction start.
- The read includes the unread portion being targeted.
- Nothing below `Begin Read` is being assumed unless it is already known complete.

This is only a partial continuation example, not a full commit-resolution example, because `End Session` does not yet reach commit.

### Safe Example 2

- Begin Session: `1`
- End Session: `500`
- Begin Read: `300`
- End Read: `500`

Why this is safe:

- The session still covers the transaction start.
- The read intentionally skips the prefix below `300`.
- That skip is safe only if everything at or below `300` is already known complete and is already loaded into inherited state.

This is the model for a safe narrow continuation.

### Not Safe Example 1

- Begin Session: `2`
- End Session: `1000`
- Begin Read: `200`
- End Read: `600`

Why this is not safe:

- The session starts after the transaction start.
- That violates the golden rule.
- Even if the commit is known, the continuation does not have a session that reaches back to the original start.

### Not Safe Example 2

- Begin Session: `600`
- End Session: `1000`
- Begin Read: `600`
- End Read: `1000`

Why this is not safe:

- The session starts after both the start and earlier transaction operations.
- The continuation can see the commit, but it cannot safely claim it has the full transaction history.
- This is the clearest violation of the golden rule.

## The Trusted Prefix Invariant

The current concurrent continuation design relies on a stronger invariant than the original golden rule:

If a continuation uses `Begin Read = X`, then the system is asserting that every transaction operation at `SCN <= X` is already known complete for that transaction.

This is the trusted prefix invariant.

If that invariant holds, a narrow bridge or continuation can safely skip the prefix and only read the suffix.

If that invariant does not hold, then a continuation that starts reading above the transaction start can miss data even while satisfying the original session-level golden rule.

## Consequence For Known-Commit Continuations

Knowing the commit location gives a safe upper bound.

It does not by itself give a safe lower bound.

So for a transaction whose commit is known:

- it is always valid to cap `End Session` and `End Read` at `COMMIT_SCN`
- it is not automatically valid to start reading at `lastReadScn`

Using `lastReadScn` as `Begin Read` is safe only if the trusted prefix invariant holds.

If the trusted prefix invariant is uncertain, the continuation must instead:

1. keep `Begin Session <= START_SCN`
2. choose a more conservative `Begin Read`
3. re-read the unsafe interval
4. invalidate any worker results that overlap that unsafe interval

This is the basis for interval-based continuation rather than immediate full serial degradation.

## Two-Thread Case

In a two-thread scenario, if a transaction starts in `log1` and the commit is only found in `log3`, the safe continuation may collapse to the same behavior as serial fallback.

That is expected.

If no worker result exists completely outside the unsafe interval, then preserving concurrency gains is not possible and the continuation becomes equivalent to a single worker reading from the transaction start through the known commit range.

This does not violate the design. It is the degenerate case of the more general rule.

## Review Checklist

When evaluating any concurrent continuation, bridge, or targeted reread, ask:

1. Does the LogMiner session begin at or before the transaction start?
2. Does the session extend far enough to include the commit being resolved?
3. Is every skipped operation below `Begin Read` already proven complete and present in inherited state?
4. If not, are we re-reading a conservative unsafe interval instead?
5. Are overlapping worker results invalidated when the continuation re-reads their range?

If the answer to question 1 is no, the golden rule is violated.

If the answer to question 3 is no, the continuation may still be unsafe even if the original golden rule appears satisfied.

## Current Design Implication

The original narrow bridge idea is correct only if the trusted prefix invariant is true.

If that invariant cannot be defended, then the safe design is not "read from `lastReadScn` to known commit".

The safe design is "start the session before the transaction start, then re-read from the first untrusted boundary through the known commit, preserving any worker work that lies fully outside that unsafe interval".