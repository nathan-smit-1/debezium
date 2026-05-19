# Concurrent LogMiner Test Backlog

This backlog focuses on Golden Rule coverage and mode-transition scenarios for the Oracle concurrent LogMiner prototype.

## Implemented in this pass

- [x] Archive plus redo followed by mixed fallback and archive-only recovery.
- [x] Unknown-commit state forces serial fallback, then concurrent mode resumes once cleared.
- [x] Bridge and replay follow-up passes run before redo serial follow-up in a mixed wave.
- [x] Redo-only windows always use bounded serial execution.
- [x] Exact-once dispatch across archive -> redo -> archive mode transitions.
- [x] Exact-once dispatch across repeated archive/redo/archive/redo/archive oscillation.
- [x] Unknown-commit transaction surviving multiple waves before resolving in serial mode.

## Highest-priority next tests

- [x] Matched orphan plus unmatched inherited transaction in the same wave.
- [x] Commit exactly on the bounded upper SCN / next-wave lower boundary.
- [ ] Multiple cross-worker transactions resolving in one bridge batch plus replay tail.
- [ ] DDL contamination replay followed by redo serial follow-up.
- [x] Bridge failure recovery restoring orphan and inherited state before retry.
- [ ] Replay failure recovery without redispatching bridged transactions.
- [ ] Worker failure during concurrent wave followed by successful serial fallback.

## Additional useful coverage

- [ ] Single-archive wave falls back to serial and still advances offsets correctly.
- [ ] Mixed archive plus redo wave with no dispatched data leaves the no-data flag set.
- [ ] Redo-only wave uses the bounded final upper SCN instead of the redo sentinel nextScn.
- [ ] Resume concurrent mode after a serial wave that also cleared pending replay units.
- [ ] Stable ordering for same-SCN commits across mode transitions.
- [ ] Cross-thread transactions where only one redo thread forces archive truncation.