<div align="center">
  <h1>seda-bus (Java)</h1>
  <p><strong>Resolving Architecture &mdash; Clarity in Design</strong></p>
  <p>A small, broker-less, <strong>staged</strong> message bus.</p>
</div>

Work is decomposed into stages (`MessageChannel`s) connected by bounded queues.
**One** shared worker pool drains every stage; each stage is capped at its own
concurrency so no stage can monopolise the pool. The only dependency is
`ra-common`.

```java
SEDABus bus = new SEDABus();
bus.start(new Properties());

bus.registerChannel("work", 1000, ServiceLevel.AtMostOnce, null, false, 4); // capacity, level, type filter, pubSub, concurrency
bus.registerAsynchConsumer("work", envelope -> {
    // ... handle it; return false to nack (retried, then dead-lettered)
    return true;
});

Envelope e = Envelope.documentFactory();
DLC.addRoute("work", "handle", e);
bus.publish(e);                       // fire and forget
bus.publish(e, reply -> { /* ... */ }); // with a completion callback

bus.gracefulShutdown();
```

## What changed in 1.3

The worker pool was the bottleneck: a 100 ms scan loop that submitted **one**
drain task per non-empty channel per tick &mdash; an effective ceiling of ~10
messages/second/channel. It is now event-driven: publishing schedules a
self-rescheduling drain task, gated by a per-stage concurrency permit. Also
fixed: a `Properties.contains` (vs `containsKey`) bug that silently disabled the
storage-location override, a round-robin race that could throw
`IndexOutOfBounds`, `pause()` that did not actually pause, static cross-instance
locks, and pub/sub subscriber channels that were queued but never drained.
Retry + dead-letter and hardened (atomic-write, ordered-replay) persistence were
added.

## Features

| | |
|---|---|
| **Bounded stages** | per-channel capacity &mdash; admission control |
| **Service levels** | `AtMostOnce` (in-memory) / `AtLeastOnce` (persisted, replayable) / `ExactlyOnce` (persisted + dedup-on-replay) |
| **Per-stage concurrency** | how many envelopes a stage may process at once |
| **Delivery** | point-to-point (round-robin) or pub/sub (fan-out to subscriber channels) |
| **Datatype channels** | optional type filter per channel |
| **Routing slips** | dynamic itinerary carried on the envelope (`ra.common` `DynamicRoutingSlip`, a LIFO stack) |
| **Retry + dead-letter** | nacked envelopes retry up to `maxAttempts`, then `deadLetter.json` |
| **Graceful shutdown** | pause, drain within a timeout, then stop the pool |

`ExactlyOnce` here means *processing* effectively once (the channel remembers a
bounded history of delivered ids and skips duplicates on replay). It is **not**
a distributed two-phase commit; the earlier Javadoc claiming one was wrong.

## What this is not

SEDA's original design included a **controller** that watched per-stage latency
and queue depth at runtime and re-tuned thread allocation and shed load
automatically. That adaptive controller is not implemented &mdash; every setting
is static configuration. It is the interesting next step (`2.0`).

## Correctness suite coverage

Per `seda-bus-design/CORRECTNESS_SUITE.md` (the language-agnostic spec every `seda-bus-*`
port verifies against). All tests below live in `SEDABusTest.java`.

| # | Property | Test(s) |
|---|---|---|
| C1 | Backpressure: Reject | `rejectsWhenChannelAtCapacity` |
| C1 | Backpressure: DropNewest | `dropNewestRejectsLikeRejectWhenFull` |
| C1 | Backpressure: DropOldest | `dropOldestEvictsInsteadOfRejecting` |
| C1 | Backpressure: Block | `blockBackpressureWaitsInsteadOfRejecting` |
| C2 | Succeeds on final attempt, delivered exactly once | `succeedsOnFinalAttemptDeliversExactlyOnce` |
| C2 | Exhausts attempts, dead-lettered | `nackRetriesThenDeadLetters` |
| C2 | No consumers eventually dead-letters (not silently discarded) | `noConsumersEventuallyDeadLettersWithoutMessageLoss` |
| C3 | Throwing consumer doesn't crash the bus or lose other envelopes | `throwingConsumerDoesNotCrashTheBusOrLoseOtherEnvelopes` |
| C4 | Shutdown accounting (bus-level; see the test's own comment for a real subtlety found while writing it) | `shutdownDoesNotReportDrainedWhileWorkIsStillInFlight` |
| C5 | Invalid capacity is clamped, not silently broken | `capacityBelowOneIsClampedToAtLeastOne` |
| C5 | Invalid maxAttempts is clamped, not silently broken | `maxAttemptsBelowOneIsClampedToAtLeastOne` |
| C6 | No thread leak across repeated create/shutdown cycles | `repeatedLifecyclesDoNotLeakThreads` |
| C7 | Concurrent producers deliver exactly once | `manyProducersDeliverEverything` |

**Known issue, not part of this suite:** `deliversPointToPoint` is flaky
under repeated back-to-back runs (bursts 20 ungated publishes against the
default capacity of 10); pre-existing, unrelated to backpressure/correctness
work, not yet fixed.

## Companion implementations

Same design, other languages:

* [seda-bus](https://github.com/resolvingarchitecture/seda-bus) &mdash; Rust, zero-dependency
* [seda-bus-python](https://github.com/resolvingarchitecture/seda-bus-python) &mdash; built to exercise free-threaded (PEP 703) CPython
* [seda-bus-ts](https://github.com/resolvingarchitecture/seda-bus-ts) &mdash; TypeScript / Node, event-loop model

## Build

```sh
mvn test        # requires ra-common 1.2.0 in the local repo
mvn package
```

## Reference

Welsh, Culler, Brewer. *SEDA: An Architecture for Well-Conditioned, Scalable
Internet Services.* SOSP 2001.
[[whitepaper]](https://github.com/mdwelsh/mdwelsh.github.io/blob/main/papers/seda-sosp01.pdf)
