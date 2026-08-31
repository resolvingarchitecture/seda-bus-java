# seda-bus — Design

## What it is

A small, broker-less, **staged** message bus. Work is decomposed into stages
(`MessageChannel`s) connected by bounded queues; one shared worker pool drains every
stage; each stage is capped at its own concurrency so none can monopolise the pool.

Reference: Welsh, Culler, Brewer. *SEDA: An Architecture for Well-Conditioned,
Scalable Internet Services.* SOSP 2001.

The only dependency is `resolvingarchitecture:common` (`Envelope`,
`DynamicRoutingSlip`, the `messaging` interfaces).

## Components

    SEDABus            implements ra.common.messaging.MessageBus
      namedChannels    channel registry, keyed by name (== service name)
      callbacks        producer completion callbacks, keyed by envelope id
      pool             the one WorkerThreadPool
      publish(e)       -> lookupChannel(e) -> channel.send(e) -> pool.schedule(ch)
      completed(e)     THE ROUTING-SLIP ENGINE (below)

    SEDAMessageChannel  one stage: a bounded ArrayBlockingQueue + its consumers
      capacity          admission control; a full queue fails send() (back-pressure)
      serviceLevel      AtMostOnce | AtLeastOnce | ExactlyOnce
      dataTypeFilter    optional per-channel content-type filter
      pubSub            fan-out to subscription channels, or point-to-point round-robin
      maxAttempts       nacked envelope retried, then dead-lettered
      persistence       AtLeastOnce/ExactlyOnce: atomic-write before enqueue,
                        ordered replay on sendUnprocessed(), dedup on ExactlyOnce

    WorkerThreadPool    one fixed pool shared by every stage
      permits           one Semaphore per channel = that stage's concurrency cap
      schedule(ch)      event-driven: submit a drain task iff work + a free permit
      drain(ch)         process up to BATCH envelopes, release permit, re-schedule

There is **no polling loop**. Publishing schedules a self-rescheduling drain task,
gated by a per-stage concurrency permit.

## The routing-slip engine

An `Envelope` carries a `DynamicRoutingSlip` — a LIFO stack of `Route`. The producer
pushes routes (`Envelope.addRoute` / `addExternalRoute`) and calls `ratchet()` once.

`SEDABus.publish` resolves the target channel from the current route's `service`.
When a channel finishes an envelope it calls `SEDABus.completed(e)`:

    if slip.peekAtNextRoute() != null:
        e.ratchet()                       # pop the next Route into currentRoute
        schedule the channel named by the new currentRoute.getService()
    else:
        fire the producer's Client callback (end of itinerary)

So a consumer advances an envelope simply by pushing a route and returning.

## Delivery semantics

| level        | on `send()`                              | on crash mid-delivery            |
|--------------|-----------------------------------------|---------------------------------|
| `AtMostOnce` | queued in memory, returns immediately   | envelope lost                   |
| `AtLeastOnce`| written to the channel's store first    | replayed; consumers must be idempotent |
| `ExactlyOnce`| as AtLeastOnce + delivered-id history   | replayed, duplicates skipped    |

`ExactlyOnce` means *processing* effectively once (a bounded id history). It is not a
distributed two-phase commit.

## What this is not

SEDA's original design had a **controller** that watched per-stage latency and queue
depth at runtime and re-tuned thread allocation and shed load automatically. That
adaptive controller is not implemented — every setting is static config. It is the
interesting next step (see [`TODO.md`](TODO.md)).

## Concurrency notes

- One envelope is owned by one stage at a time; `DynamicRoutingSlip` / `DequeStack`
  are not thread-safe and do not need to be.
- Worker threads are daemon threads (`seda-worker-N`).
- Channel registration and pub/sub subscription are synchronized; publish and drain
  are lock-free on the hot path (a `ConcurrentHashMap` registry + per-stage
  `Semaphore`).
