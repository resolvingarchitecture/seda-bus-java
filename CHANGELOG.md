# Changelog

## Unreleased
- Fixed: an envelope is no longer lost when the next stage cannot take it. `SEDABus.completed` finds the next stage
  before moving the envelope's route on, and puts the route back if the stage refuses (full, paused or unknown); the
  channel acknowledges (deletes the persisted copy) only after the next stage accepted, retries only the hand-over (the
  stage's own work is not run again), and dead-letters after `maxAttempts`. `ExactlyOnce` marks an envelope delivered
  only after the hand-over.
- Fixed: dead-lettering on a channel with no store directory (AtMostOnce) discarded the envelope silently. It is now
  kept in a bounded in-memory list (`SEDAMessageChannel.inMemoryDeadLetters()`, 1,000 most recent, lost on restart) and
  counted (`deadLetterCount()`), and a warning names it.
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
