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

`ExactlyOnce` means *processing* effectively once (a bounded history of delivered ids, the most recent
100,000 per channel). It is not a distributed two-phase commit.

The history is kept in `delivered.log` in the channel's store directory. An id is appended (synchronously) after
the next stage has accepted the envelope and before the persisted copy is deleted, and the log is read back when
the channel starts, so a restart does not forget what was delivered. The log is rewritten with the most recent ids
when it holds twice the history. Two limits remain:

- A crash after the next stage accepted the envelope and before its id was recorded replays the envelope, so the
  next stage can see it twice. Closing that window needs the next stage to take part in the decision.
- An `ExactlyOnce` envelope on a channel with no store directory (`AtMostOnce`) has an in-memory history only.

Recording each id costs one synchronous disk write per `ExactlyOnce` delivery.

## Retry and hand-over

A nacked envelope (the consumer returned `false`) is retried up to `maxAttempts` (default 3), then dead-lettered. When
the stage has done its work but the next stage refuses the envelope (full, paused, or not registered yet), only the
hand-over is retried, not the consumer, for up to `handOverAttempts` (default 10), because pressure downstream is
usually temporary.

Retries wait: the first after 50 ms, each next one twice as long, up to 2 s, give or take a fifth. The wait happens on
the bus's own retry timer (one daemon thread per bus, started with the bus and stopped with it), so no worker is held
while an envelope waits, a stage that is slow to recover does not stall the others, and one bus's retries never wait
behind another's. An envelope waiting for its retry is out of the queue, so `queued()` does not count it; use
`pendingRetries()`.

Settings are properties of the bus, applied to every channel it registers: `ra.sedabus.retry.baseMs` (50),
`ra.sedabus.retry.maxMs` (2000) and `ra.sedabus.retry.handOverAttempts` (10). A single channel can be changed with
`setRetryBackoff(baseMs, maxMs)` and `setHandOverAttempts(n)` on the `SEDAMessageChannel` that `SEDABus.getChannel(name)`
returns.

**When an envelope is given up on** (its retries ran out, or `DropOldest` pushed it out of a full queue): it is
dead-lettered (a persisted channel writes `deadLetter.json`, others keep the 1,000 most recent in memory), the
producer's callback is released and replied to with an error message on the envelope, and the bus's
`DeadLetterListener` (`SEDABus.setDeadLetterListener`) is called with the channel, the envelope and the reason. Counters:
`deadLetterCount()`, `evictedCount()`, `pendingRetries()`.

**When the bus stops with retries waiting** (a graceful shutdown waits up to 30 s for them, `shutdown()` 3 s): a
persisted envelope keeps its copy on disk and is replayed at the next start; one that was never persisted (`AtMostOnce`)
is given up on as above, with the reason "stopped before this envelope could be retried". A stopped bus accepts nothing
new, and a retry that comes up for scheduling after the timer has stopped is given up on in the same way.

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
