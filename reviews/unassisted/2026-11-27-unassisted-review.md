# Unassisted review: seda-bus-java

An architecture review of the implementation model alone, written without the Meridian service, to compare with Meridian's
reviews in `reviews/full/`. The review below is the reviewer's text, unedited.

| Item        | Value                                                                                                                                                  |
|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| Date        | 2026-11-27                                                                                                                                             |
| Prompt      | `provide an architecture review of this implementation model` (the whole prompt; nothing else was given)                                               |
| Reviewer    | A new Claude Code session on `claude-opus-5-5`, with no earlier conversation, memory or project instructions                                           |
| Files given | `src/` (4 main classes, 1 config file, 2 test classes), `pom.xml`, `.github/workflows/ci.yml`, `README.md`, `DESIGN.md`, `TODO.md`, `CHANGELOG.md`     |
| Not given   | Any Meridian rule, review, report or score; the Meridian design model (goals, stakeholders, decisions, risks, requirements); the `reviews/` folder     |
| Tools used  | Read-only file reading and listing through the shell; it also extracted the `ra-common` 1.2.0 jar from the local Maven repository to inspect one class |
| Duration    | About 4 minutes                                                                                                                                        |
| Not done    | It did not build the project or run its tests                                                                                                          |

---

# Architecture review: seda-bus-java (1.3.1 plus the Unreleased changes)

**Scope:** `SEDABus`, `SEDAMessageChannel`, `WorkerThreadPool`, `Backpressure`, the docs and the tests. I read the code and checked one thing in the `ra-common` 1.2.0 bytecode (how `Envelope.envelopeFactory` copies an envelope). I did not run the build or the tests.

## Summary

The overall shape is sound. A single shared pool with a concurrency cap per stage, draining driven by events, and a routing slip as the itinerary is a sensible small form of SEDA. Earlier rounds fixed a lot. The "lost on hand-over" fix, where the persisted copy is deleted only after the next stage accepts, is the right idea.

What's left is mostly at the seams, not in the components themselves:
- shared state between pub/sub copies
- blocking inside a shared pool
- replay that skips the stage machinery
- a lifecycle that doesn't return to a working state

These matter more than the missing adaptive controller.

## High: correctness or liveness at the architecture level

**1. Pub/sub copies share the routing slip, the headers and the id.**
`fanOut` (`SEDAMessageChannel.java:516`) calls `Envelope.envelopeFactory(envelope)`. In ra-common 1.2.0 that passes the original's `getId()`, `getHeaders()` and `getDynamicRoutingSlip()` by reference. Three things follow:
- Every subscriber copy, and the original, call `ratchet()` / `addRoute()` on the same `DynamicRoutingSlip` from different worker threads. `DESIGN.md` says "one envelope is owned by one stage at a time", and this breaks that. The slip isn't thread-safe.
- Producer callbacks are keyed by id (`SEDABus.callbacks`). Whichever copy finishes first fires the producer's callback. The others find nothing, or, if one fails, reply with an error after the success has already been reported.
- If any subscriber refuses (`all = false`), the whole fan-out is retried. Subscribers that already accepted get it again.

**Recommendation:** fan-out has to deep-copy the slip and the headers and give each copy its own id, with a parent/correlation id. Decide whether the producer callback means "all subscribers done" or "fan-out accepted". Retry per subscriber, not per fan-out.

**2. `Backpressure.Block` can deadlock the shared pool.**
`admit` uses `queue.put(e)` with no timeout (`SEDAMessageChannel.java:254`). That runs on the producer's thread, but also on worker threads, through `completed() → next.send()` (`SEDABus.java:135`) and through fan-out. Suppose stage A's concurrency (or several stages' combined) reaches the pool size and every worker is blocked in `put` on a full stage B. Then nothing is left to drain B, and the pool stops for good. A cycle in the stage graph also deadlocks.

Shutdown can't release it either: `pause()`/`shutdown()` set `accepting=false` but don't wake threads already blocked in `put`. The enum's Javadoc mentions "up to the send timeout, if any", but no send timeout exists.

**Recommendation:** never block a worker. Inside the bus, treat a refused hand-over as Reject and use the retry timer you already have. Let `Block` apply only to external producers, with a timeout, and wake those producers on shutdown. Alternatively, make sure the sum of concurrency over stages that use Block stays below the pool size.

**3. Replay goes around the stage and duplicates live work.**
`sendUnprocessed` (`SEDAMessageChannel.java:672-699`) has three problems:
- It calls `process(e)` directly on the caller's thread. That skips the queue, the capacity limit, the concurrency permit and the pool. A large backlog replays one envelope at a time, synchronously, inside `resumeUnprocessed()`.
- It lists every `*.json` in the store, and every guaranteed envelope that is queued, in flight or waiting to retry has a file there. Calling `resumeUnprocessed()` on a running bus processes those envelopes a second time.
- `fromJSON` (`:690`) can throw on a truncated or corrupt file. That ends the whole replay loop; it is not skipped like an `IOException`.

**Recommendation:** make replay a startup-only step that feeds files back through `admit()` and the pool. Refuse it while running, or exclude ids already tracked as in flight. Isolate any file that fails to parse into a quarantine directory instead of aborting.

**4. Lifecycle state doesn't survive `restart()`, and registering before `start()` loses settings.**
- `restart()` (`SEDABus.java:381`) runs `shutdown()` then `start()`. Shutdown leaves every channel with `accepting=false`, and `start()` never unpauses or restarts them, so a restarted bus rejects everything.
- The new `WorkerThreadPool` has no permits registered. `schedule` then uses `computeIfAbsent(..., new Semaphore(1))` (`WorkerThreadPool.java:66`), so every stage's concurrency silently drops to 1.
- The same happens to a channel registered before `start()`: `pool == null` at `SEDABus.java:300`, so its concurrency is never recorded, and it starts with `config == null` (the default store directory).
- Calling `start()` twice leaks a pool and a retry timer.

**Recommendation:** have the bus own a registry of channel specs (name, capacity, concurrency, backpressure and so on) that it re-applies on `start()`. Make the lifecycle an explicit state machine, with compare-and-set transitions that reject illegal moves.

## Medium: semantics that don't match the docs

**5. Pausing the bus turns multi-stage traffic into dead letters.**
`pause()` stops *admission* (`accepting=false`), but the pool keeps draining. Envelopes already queued get processed, their hand-over to the paused next stage is refused, and the retry timer re-queues them even while paused (`queue.offer` at `:448` ignores `accepting`). The hand-over budget is 10 attempts, about 9 s in total (50 + 100 + … + 2000 ×3). So any pause longer than about 9 s dead-letters every envelope in flight between stages.

**Recommendation:** split pause into two separate operations, stopping admission from outside and stopping draining. While paused, keep the hand-over budget from counting down.

**6. A service level on the envelope can't raise the channel's guarantee.**
`send` honours `e.getServiceLevel()` (`:219`). But `channelDir` exists only when the *channel* is guaranteed, and `persist()` returns `true` when there is no directory (`:787`). So an `AtLeastOnce` envelope on an `AtMostOnce` channel reports success and is never persisted. `ack` checks the channel level, `send` checks the envelope level, and `markDelivered` checks either.

**Recommendation:** pick one source of truth. Either reject an envelope whose level is above the channel's, or always open a store directory.

**7. The persistence guarantees are weaker than "atomic-write" suggests.**
- **No fsync before the rename:** `persist()` (`:786`) writes a temp file and renames it atomically, but syncs neither the file nor the directory. The rename is atomic, but after a power loss the file can be empty. Yet `delivered.log` is written with `SYNC`, so the system is only as durable as its weaker half.
- **Compaction can drop ids:** `appendDelivered` isn't synchronised against `compactDeliveredLog` (`:559` / `:576`). An id appended between the snapshot and the `ATOMIC_MOVE` disappears from disk, which defeats ExactlyOnce after the next restart.
- **Each ack scans the whole directory:** `removePersisted` (`:808`) lists the full store directory to find the file. With a backlog, the cost grows with the square of its size.
- **Unsafe file naming:** the sequence counter `PERSIST_SEQ` is shared across the whole JVM. Once it overflows it goes negative and `%08d` stops sorting correctly.

**Recommendation:** keep a map from id to file path in memory. Fsync the file and then the directory. Run append and compaction under one lock, or use a log that only grows, split into segments. In time, replace one file per envelope with a write-ahead log per channel. That is also the natural place for the `deadLetter.json` rotation listed in the TODO.

**8. Shutdown is inconsistent and slow.**
- **Guaranteed retries get dead-lettered:** if a retry comes due after the timer has stopped, `scheduleRetry` hits `RejectedExecutionException` (`:458-462`). It calls `giveUp`, which dead-letters the envelope and *deletes its persisted copy*, even for guaranteed channels. `DESIGN.md` says such envelopes are kept for replay, and `abandonPendingRetries` does keep them.
- **Queued AtMostOnce envelopes vanish:** when the pool stops, any still queued in AtMostOnce channels are dropped without a dead letter or a callback.
- **Drain time adds up per channel:** `doShutdown` drains the channels one after another (`SEDABus.java:395-401`), so the worst case is N × 30 s. It also pauses each downstream stage while upstream stages are still handing work to it.

**Recommendation:** shut down in topological order, or stop admission everywhere first and then drain against one shared deadline. Send every envelope that is abandoned through the same path.

## Structural / API

**9. The channel abstraction leaks.** `SEDAMessageChannel` is typed against `MessageBus`, but it uses `instanceof SEDABus` casts to reach `failed`, `scheduleRetry` and `wake` (`:280`, `:439`). With any other bus, retries silently become dead letters. Pass a small internal `StageContext` interface (retry scheduler, failure sink, wake-up) to the channel instead.

**10. The "pull model" isn't really one.** The public `receive()`/`poll()` (`:333`, `:360`) also *process* the envelope through the registered consumers, and they compete with the pool without taking a permit. A channel with no consumers can't be pulled from: its envelopes are retried and then dead-lettered. Either remove these from the public surface or give the pull model real behaviour (take an envelope, and the caller acks or nacks it).

**11. Registration has too many positional overloads.** There are seven `registerChannel` variants with up to eight positional parameters and a raw `Class`. `maxAttempts` can't be set through the bus at all; it is fixed at `3` in `SEDABus.java:268`. A `ChannelSpec` builder would fix this and also feed the registry from item 4.

**12. Ownership is handed over loosely.** After `next.send(e)` succeeds, the finishing stage still calls `markDelivered(envelope)` / `ack(envelope)` on an envelope the next stage may already be working on. Today that is safe only because those calls read `getId()`/`getServiceLevel()`. Copy the id before the hand-over so the ownership rule is real, not accidental.

## Observability, tests, docs

- **No metrics yet:** the 2.0 controller needs per-stage queue depth, service time and wait time. None is collected, and the counters that exist are spread around. Add the `ChannelMetrics` hook before the controller; it is the cheaper prerequisite.
- **Tests:** the timing-based `Thread.sleep` waits and the lack of crash/fault-injection tests are already in `TODO.md`. I'd add a regression test for each of items 1 to 4: pub/sub plus a routing slip, Block with concurrency ≥ pool size, `resumeUnprocessed` while running, and `restart()` followed by `publish`.
- **Doc drift:**
  - `TODO.md` still lists "Backoff between retry attempts (currently immediate re-queue)", which is done.
  - The components section of `DESIGN.md` doesn't mention `Backpressure`.
  - The `SEDABus` Javadoc advertises a pull model that doesn't behave as one (item 10).

## Suggested order

1. Pub/sub copy isolation (1)
2. No blocking on workers (2)
3. Replay through the stage, at startup only (3)
4. Lifecycle and channel-spec registry (4, 11)
5. Pause semantics (5)
6. Durability fixes (7)

Items 1–3 can silently corrupt, duplicate or hang work, so they come before any work on the adaptive controller.
