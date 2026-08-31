# seda-bus — TODO

## Done

- [x] Event-driven worker pool with per-stage concurrency permits (1.3.0).
- [x] Retry + dead-letter; atomic-write + ordered-replay persistence;
      ExactlyOnce dedup-on-replay (1.3.0).
- [x] JUnit suite (1.3.0).
- [x] Compile target lowered to Java 11 (1.3.1).

## Next

### 2.0 — the adaptive controller

The headline missing piece of the SEDA design.

- [ ] Per-stage instrumentation: queue depth, wait time, service time, throughput,
      nack rate — sampled cheaply on the drain path.
- [ ] A controller thread that periodically re-tunes each stage's concurrency permit
      from measured latency vs. a target.
- [ ] Load shedding: when a stage is saturated, reject at admission (already the
      mechanism) and optionally signal upstream stages to slow.
- [ ] Config to cap total threads and per-stage min/max concurrency.
- [ ] A pluggable controller policy so callers can supply their own.

### Smaller

- [ ] Metrics hooks (a `ChannelMetrics` listener) so hosts can export to their own
      telemetry without the controller.
- [ ] Configurable `BATCH` size per channel (currently a constant 64).
- [ ] Backoff between retry attempts (currently immediate re-queue).
- [ ] Dead-letter file rotation (channel `deadLetter.json` grows unbounded).
- [ ] `receive(timeout)` / pull-model ergonomics: a typed `poll` that returns the
      handler result.
- [ ] Optional priority within a stage (currently strict FIFO per channel).
- [ ] Document the persistence file format and the replay ordering guarantees.

### Housekeeping

- [ ] Publish to a real repository (currently local / jitpack only).
- [ ] Property-based tests for the routing-slip engine and for replay under
      simulated crashes.
- [ ] A couple of the capacity / retry / dead-letter tests are timing-sensitive and
      flake under load (pass reliably in isolation). Make them deterministic
      (inject a clock / use latches instead of sleeps).
