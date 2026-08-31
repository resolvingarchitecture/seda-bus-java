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

## Companion implementations

Same design, other languages:

* [seda-bus](https://github.com/resolvingarchitecture/seda-bus) &mdash; Rust, zero-dependency
* [seda-bus-python](https://github.com/resolvingarchitecture/seda-bus-python) &mdash; built to exercise free-threaded (PEP 703) CPython

## Build

```sh
mvn test        # requires ra-common 1.2.0 in the local repo
mvn package
```

## Reference

Welsh, Culler, Brewer. *SEDA: An Architecture for Well-Conditioned, Scalable
Internet Services.* SOSP 2001.
[[whitepaper]](https://github.com/mdwelsh/mdwelsh.github.io/blob/main/papers/seda-sosp01.pdf)
