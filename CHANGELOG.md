# Changelog

## Unreleased
- Fixed: an envelope is no longer lost when the next stage cannot take it. `SEDABus.completed` finds the next stage
  before moving the envelope's route on, and puts the route back if the stage refuses (full, paused or unknown); the
  channel acknowledges (deletes the persisted copy) only after the next stage accepted, retries only the hand-over (the
  stage's own work is not run again), and dead-letters after `maxAttempts`. `ExactlyOnce` marks an envelope delivered
  only after the hand-over.
- Fixed: retries shared one static timer thread across every bus in the process, never stopped; each bus now owns its
  timer (started and stopped with it), so one bus's retries do not wait behind another's and none fire after shutdown. A
  retry still waiting at shutdown is no longer cancelled without trace: a persisted envelope keeps its copy for the next
  start, any other is given up on and reported.
- Added: the settings and counters are reachable. `SEDAMessageChannel` is public and `SEDABus.getChannel(name)` returns it;
  retry settings are bus properties (`ra.sedabus.retry.baseMs`, `.maxMs`, `.handOverAttempts`); `pendingRetries()` counts
  envelopes waiting out a retry delay; `SEDABus.setDeadLetterListener` is told when an envelope is dead-lettered or evicted.
- Fixed: retries were immediate, so a briefly full or paused next stage turned into dead letters within moments.
  Retries now back off (50 ms, doubling to 2 s, with jitter) on a timer thread, without holding a worker. A refused
  hand-over is retried up to `handOverAttempts` (default 10) times, separate from `maxAttempts`, and only the hand-over
  is retried. `setRetryBackoff` and `setHandOverAttempts` change the defaults.
- Fixed: the `ExactlyOnce` delivered-id history was in memory only, so a restart forgot it and replayed envelopes were
  delivered again. It is now written to `delivered.log` in the channel's store directory (synchronously, after the next
  stage accepted the envelope and before the persisted copy is deleted), read back at start, and compacted to the most
  recent 100,000 ids. A crash between the next stage accepting an envelope and its id being recorded can still
  replay it; this is documented in `DESIGN.md`.
- Fixed: dead-lettering on a channel with no store directory (AtMostOnce) discarded the envelope silently. It is now
  kept in a bounded in-memory list (`SEDAMessageChannel.inMemoryDeadLetters()`, 1,000 most recent, lost on restart) and
  counted (`deadLetterCount()`), and a warning names it.
- Fixed: `DropOldest` left the evicted envelope's persisted file behind, so it was replayed after a restart; the file is
  now deleted, the eviction is logged and counted (`SEDAMessageChannel.evictedCount()`), and the producer is told.
- Fixed: `SEDABus` kept the producer callback of an envelope that was dead-lettered, so the map grew and the producer
  waited for ever. The callback is released and the producer's reply carries an error message saying what happened.
- Fixed: `deliversPointToPoint` was flaky (20 publishes into a channel of capacity 10); it now has room for the burst.
- Fixed: the first dead letter could never be written (`deadLetter.json` was opened append-only without creating it).

## 1.3.1
- Compile target lowered Java 17 -> 11 (the code uses no post-11 features). Keeps
  seda-bus consumable by Java 11 downstreams (`service-bus`, `1m5-desktop-java`).
- Added `DESIGN.md` and `TODO.md`.

## 1.3.0
- Rewrote the worker pool: event-driven drain with per-stage concurrency permits, replacing the 100ms scan loop (removes the ~10 msg/s/channel ceiling).
- Fixed: Properties.contains vs containsKey; round-robin IndexOutOfBounds race; pause() not pausing; static cross-instance locks; pub/sub subscriber channels never drained.
- Added: retry + dead-letter; atomic-write + ordered-replay persistence; ExactlyOnce dedup-on-replay (and corrected the misleading two-phase-commit Javadoc).
- Added a real JUnit suite. Bumped to Java 17; pinned plugin versions.

## 1.2.0
- Prior release.
