---
service: REVIEW_INTERMEDIATE
project_version: 1.3.1
version_source: pom.xml
reviewed_at: 2026-11-26T06:07:59.984698341Z
architecture_model_version: 8
project_id: 405581ad-f00f-459f-8629-ad6f59923b81
evaluation_id: 7a4ded83-c1ac-4bf5-a1bb-49d2c7ad8917
---

# Change Review

## Summary

🟡 **Overall.** 🟡 0 to fix now · 🟡 4 that could cause problems later · 🟢 2 minor

*🔴 fix now: a high-severity problem, drift that is likely and serious, or a full review required. 🟡 could cause trouble later. 🟢 fine, or improves quality without being pressing.*

🟢 **No full review needed.** The change fits the current architecture.

The change closes most loss paths flagged in the last full review without structural drift. It introduces undocumented semantics: callbacks fired on failure, a shared static retry timer, and new settings hosts cannot reach. Next, record these in an ADR update and the shared suite, tighten the C2 and shutdown tests, and submit test results.

## Architectural Drift

🟢 **No drift found** (severity low), confidence 70%.

The change closes loss and unbounded-growth paths that the 02:40 full review flagged, inside the accepted staged, broker-less, service-level and backpressure decisions. It adds no component or external dependency. It does add behaviours that no accepted decision or shared suite case covers: a shared static retry timer, producer callbacks fired on failure, and a persisted dedup log.

**Baseline assumptions affected:**

- No envelope is lost or delivered twice under concurrent load, retries and shutdown
- Exhausted retries are dead-lettered, never silently discarded
- Bounded memory and predictable behaviour when a stage is saturated
- Behaviour matches the shared CORRECTNESS_SUITE.md across all ports

**Architecture areas affected:**

- SEDAMessageChannel retry, dead-letter and dedup path
- SEDABus producer-callback handling
- Service levels and persistence (delivered.log)
- Shutdown and drain accounting

**Evidence:**

- SEDAMessageChannel.java: deadLetter now keeps a bounded in-memory list when channelDir is null
- SEDAMessageChannel.java: evicted() deletes the persisted copy under DropOldest and notifies the producer
- SEDAMessageChannel.java: delivered ids are appended to delivered.log with SYNC and reloaded at start
- SEDAMessageChannel.java: retries run on a static single-thread timer with backoff; hand-over gets its own attempt budget
- SEDABus.java: new failed() releases the callback and replies with an error message

## Needs Attention

- 🟡 **Producer callback is now also called on failure, with no separate signal** — `src/main/java/ra/sedabus/SEDABus.java`
  - failed() calls client.reply(e) when an envelope is dead-lettered or evicted, and only an error message on the envelope marks it as a failure. Callers that treat reply as completion will read failure as success.
  - If left: 1M5 and service-bus could wrongly treat dead-lettered work as done. The other six ports may not make the same change, which puts the 7-of-7 conformance goal at risk.
  - Do: Agree the failure-notification contract with the consumers and add it to CORRECTNESS_SUITE.md. Consider a distinct failure signal rather than reusing reply.
  - Decision: decision-c02acb53
- 🟡 **New knobs and counters cannot be reached by host code** — `src/main/java/ra/sedabus/SEDAMessageChannel.java`
  - setRetryBackoff, setHandOverAttempts, deadLetterCount, evictedCount and inMemoryDeadLetters are public methods on a package-private class. registerChannel returns the MessageChannel interface, so only same-package tests can call them.
  - If left: CHANGELOG and DESIGN.md describe these as configurable and observable, but hosts cannot use them. That overstates the guarantees to readers.
  - Do: Expose a small public listener or stats interface, or state in the docs that the settings are internal. Keep the one-dependency rule.
- 🟡 **Retries share one static timer thread across all buses** — `src/main/java/ra/sedabus/SEDAMessageChannel.java`
  - RETRY_TIMER is a static single-thread executor that is never shut down. Every channel's retries, dead-letter file writes and producer callbacks run on it.
  - If left: One slow callback or disk write delays retries on every bus in the process. It also reintroduces cross-instance shared state, which release 1.3.0 removed. Retries can fire after their bus has shut down.
  - Do: Make the timer owned by the bus and stopped on shutdown, or hand the work to the shared pool. Record the choice.
  - Decision: decision-f1633947
- 🟡 **Waiting retries are invisible to queued(), weakening tests and shutdown accounting** — `src/main/java/ra/sedabus/SEDAMessageChannel.java`
  - An envelope waiting for retry is out of the queue, so queued() reads 0. noConsumersEventuallyDeadLettersWithoutMessageLoss asserts only queued()==0, so it now passes without showing a dead letter. A non-graceful shutdown (3 s drain) can leave AtMostOnce retries unfinished.
  - If left: Mandatory reliability checks C2 and C4 prove less than the README claims. Envelopes can be lost silently at shutdown.
  - Do: Assert on deadLetterCount or the dead-letter record in the C2 tests. Document the shutdown fate of pending retries per service level.
- 🟢 **Persisted dedup log: per-delivery sync write, unversioned format, compaction race** — `src/main/java/ra/sedabus/SEDAMessageChannel.java`
  - Each ExactlyOnce delivery does a synchronous fsync write on a shared worker. compactDeliveredLog is synchronized but appendDelivered is not, so an id appended during a rewrite can be missing from the log. The format has no version and the new durability path has no benchmark.
  - If left: Throughput drops on ExactlyOnce stages, and a restart can rarely forget an id. The crash window that remains is documented honestly in DESIGN.md.
  - Do: Serialise append and compaction, add a format version, and benchmark ExactlyOnce throughput. Add a crash-injection test.
  - Decision: decision-de5fe60a
- 🟢 **In-memory dead letters resolve the AtMostOnce gap only partly** — `src/main/java/ra/sedabus/SEDAMessageChannel.java`
  - The sink keeps the last 1,000 whole envelopes per channel, bounded by count and not by size. It is lost on restart, and the only way to read it is the unreachable inMemoryDeadLetters() method.
  - If left: Envelopes are no longer silently discarded, but hosts cannot act on them. Memory use can still be high with large payloads.
  - Do: Record this as the chosen AtMostOnce dead-letter behaviour and add a host-visible hook. Consider a size bound.
  - Decision: decision-de5fe60a

## Decisions This Change Touches

- *adr update recommended*: Consider recording the new failure semantics: hand-over retry budget and backoff, in-memory dead-letter sink for AtMostOnce, persisted ExactlyOnce dedup, and producer notification on give-up. Also consider superseding decision-9475f172, since the flaky test is now fixed and the risk can be updated.
  - Options considered: Java-only behaviour documented in the README; Shared semantics added to CORRECTNESS_SUITE.md for all ports; Revert parts of the change
  - Trade-offs: Shared semantics cost cross-port work but protect the design as the product; Java-only behaviour is cheaper but risks divergence
  - Needs sign-off from: Brian, Resolving Architecture (owner and maintainer)
  - Open questions: Does CORRECTNESS_SUITE.md already specify any of these behaviours?
- *stakeholder review recommended*: Consider reviewing the callback contract change with the 1M5 and service-bus owners before release. Callbacks now fire on failure as well as success.
  - Options considered: Keep reply with an error message; Add a separate failure callback; Keep the old behaviour and report failures through a hook
  - Trade-offs: Reusing reply is simple but ambiguous; A new callback is clearer but widens the API
  - Needs sign-off from: Brian, Resolving Architecture (owner and maintainer); 1M5 platform and service-bus (internal consumers)
  - Open questions: Do consumers inspect error messages on the replied envelope?
- *supporting design guidance*: Consider making retry-timer ownership and the shutdown fate of pending retries explicit. Both are currently implicit in the code.
  - Options considered: Per-bus timer stopped on shutdown; Reuse the shared worker pool; Keep the static timer and document it
  - Trade-offs: Per-bus ownership removes shared state but adds a thread per bus; The shared pool risks holding workers during waits
  - Needs sign-off from: Brian, Resolving Architecture (owner and maintainer)
  - Open questions: Should pending AtMostOnce retries be dead-lettered or dropped at shutdown?

## Improve the Review

- Do 1M5 or service-bus treat a producer reply as successful completion?
- Is the timer thread meant to be shared by all buses in a process?
- Should the 'never silently discarded' NFR apply to AtMostOnce channels, and is a bounded in-memory sink enough?
- Do the C2 tests now assert on dead-letter records instead of queued()==0?
- Will persisted dedup and failure notification be added to the shared suite for all ports?
- Test or CI results for the new and existing tests after this change
- Whether CORRECTNESS_SUITE.md covers refused hand-over, failure notification and persisted dedup
- How the other six ports handle these cases
- Whether 1M5 or service-bus depend on reply meaning success
- Intended shutdown behaviour for pending retries on AtMostOnce channels

## Notes

- No build or test execution results were supplied, so correctness of this change is unverified.
- Still open from earlier reviews: Block policy can hold a shared worker inside completed() and has no timeout, and the dead-letter file is unrotated.
- Only the Java port is visible; cross-port behaviour cannot be assessed.
- Line numbers are not given because the diff was reviewed without line offsets.

---

Meridian recommends. The customer organization decides.

*This review cost 4 credits (about $2.00). 1316 credits remain.*

