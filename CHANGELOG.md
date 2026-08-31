# Changelog

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
