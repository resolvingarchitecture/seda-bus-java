---
service: REVIEW_INTERMEDIATE
project_version: 1.3.1
version_source: pom.xml
reviewed_at: 2026-11-26T04:04:40.573652953Z
architecture_model_version: 8
project_id: 405581ad-f00f-459f-8629-ad6f59923b81
evaluation_id: a03ae46f-8b22-414a-bdd1-ba6f8f602c76
---

# Change Review

## Summary

**No full review needed.** The change fits the current architecture.

The change closes the main ack-before-forward loss path in the routing-slip engine and fixes first-write dead-lettering, consistent with the accepted decisions and with no structural drift. It leaves AtMostOnce dead-lettering as a no-op, adds a duplicate window after a crash, and its semantics are not yet in the shared suite or an ADR. Next, record the semantics, decide the AtMostOnce dead-letter behaviour, and submit test results.

## Architectural Drift

**No drift found** (severity low), confidence 70%.

The change closes a loss path in the accepted routing-slip and service-level decisions. It adds no component, dependency or boundary. Its new hand-over semantics are not yet captured in a decision or in the shared correctness suite.

**Baseline assumptions affected:**

- No envelope is lost or delivered twice under concurrent load, retries and shutdown
- Routing slip carried on the Envelope; the bus advances it on completion
- Exhausted retries are dead-lettered, never silently discarded
- Behaviour matches the shared CORRECTNESS_SUITE.md across all ports

**Architecture areas affected:**

- SEDABus.completed (routing-slip engine)
- SEDAMessageChannel retry, ack and dead-letter path
- Service levels and persistence

**Evidence:**

- SEDABus.java: completed() now looks up the next stage first and restores the route if the stage refuses
- SEDAMessageChannel.java: ack and markDelivered now run only after bus.completed returns true
- SEDAMessageChannel.java: deadLetter now creates deadLetter.json on first write
- SEDABusTest.java: three new hand-over tests

## Needs Attention

- **New hand-over failure semantics are not in the shared design or suite** (medium) — `src/main/java/ra/sedabus/SEDABus.java`
  - completed() now restores the route on refusal. The channel retries only the hand-over, then dead-letters. None of this is in DESIGN.md, an accepted decision, or the README's C1-C7 mapping.
  - If left: The other six ports may behave differently on a refused hand-over. That puts the 7-of-7 conformance goal and Brian's fidelity concern at risk.
  - Do: Record the semantics in an ADR update and add a hand-over case to CORRECTNESS_SUITE.md. Update DESIGN.md to match.
  - Decision: decision-c02acb53
- **Dead-lettering a failed hand-over is still a no-op on AtMostOnce channels** (medium) — `src/main/java/ra/sedabus/SEDAMessageChannel.java`
  - deadLetter returns false when channelDir is null, which is the case for non-persisted channels. process then acks regardless. The new test covers only AtLeastOnce, yet the changelog says the envelope is dead-lettered.
  - If left: On AtMostOnce stages an envelope that cannot be handed on is still discarded. This contradicts the 'never silently discarded' NFR and overstates the fix.
  - Do: Add a dead-letter sink for every service level, or state the limit in the changelog and docs. Add an AtMostOnce hand-over test.
  - Decision: decision-de5fe60a
- **Ack-after-forward trades a loss window for a duplicate window** (medium) — `src/main/java/ra/sedabus/SEDAMessageChannel.java`
  - The persisted copy is deleted only after the next stage accepts. A crash between acceptance and deletion replays the envelope into the next stage. ExactlyOnce dedup is in memory and is lost in that same crash.
  - If left: Downstream stages can see duplicates after a crash, including at ExactlyOnce. Documentation that says only 'effectively-once' would then overstate the guarantee.
  - Do: State this window in DESIGN.md and the README. Decide whether dedup ids should be persisted.
  - Decision: decision-d7673e5e
- **Immediate hand-over retries turn transient downstream pressure into dead letters** (medium) — `src/main/java/ra/sedabus/SEDAMessageChannel.java`
  - Retries are re-queued with no backoff, and maxAttempts defaults to 3. A full or paused next stage will usually still refuse, so the envelope is dead-lettered within moments. A Block policy on the next stage can also hold a shared worker inside completed().
  - If left: Backpressure on one stage can cause dead letters upstream. Blocked workers can starve the shared pool.
  - Do: Define the retry policy for hand-over failures, with backoff or a longer budget. Check Block-policy behaviour under saturation.
  - Decision: decision-ed9e74a2
- **Producer callback is not released or notified when a hand-over dead-letters** (low) — `src/main/java/ra/sedabus/SEDABus.java`
  - SEDABus.callbacks entries are removed only on successful completion or a rejected publish. A dead-lettered envelope leaves its callback in the map, and the producer is never told.
  - If left: The map can grow unbounded, and the producer waits forever. This weakens the bounded-memory concern.
  - Do: Remove the callback and notify or report on dead-letter. Cover it with a test.
- **New tests leave gaps and add timing sensitivity** (low) — `src/test/java/ra/sedabus/SEDABusTest.java`
  - The durable hand-over test relies on a fixed one-second sleep. There is no test for ExactlyOnce marking, AtMostOnce hand-over failure, or concurrent hand-over. TODO.md already records timing-sensitive tests flaking under load.
  - If left: A flaky or incomplete test weakens confidence in the suite, which is the same trust issue as the deliversPointToPoint risk.
  - Do: Use latches or polling instead of sleeps. Add AtMostOnce and ExactlyOnce hand-over cases.

## Decisions This Change Touches

- *adr update recommended*: Consider recording hand-over failure semantics as an update to the routing-slip and service-level decisions: when the ack happens, what is retried, and when the envelope is dead-lettered. Propagate them to CORRECTNESS_SUITE.md.
  - Options considered: Java-only behaviour, documented in the Java README; Shared semantics in the correctness suite for all ports; Revert to ack-before-forward
  - Trade-offs: Shared semantics cost cross-port work but protect the design as the product; Java-only behaviour is cheaper but risks divergence
  - Needs sign-off from: Brian, Resolving Architecture (owner and maintainer)
  - Open questions: Does CORRECTNESS_SUITE.md already cover a refused hand-over?
- *architectural decision candidate*: Decide where an envelope goes when it is dead-lettered on a channel with no persistence directory (AtMostOnce). This is for the owner to decide.
  - Options considered: Bounded in-memory dead-letter sink; Failure or dead-letter callback hook; Create the directory for all levels; Document discard for AtMostOnce
  - Trade-offs: A sink or hook adds API surface; Writing to disk for AtMostOnce adds I/O cost to a level meant to avoid it; Documenting discard is cheap but conflicts with 'never silently discarded'
  - Needs sign-off from: Brian, Resolving Architecture (owner and maintainer)
  - Open questions: Is the NFR intended to cover non-persisted channels?
- *supporting design guidance*: State in the public docs the duplicate window created by ack-after-forward and the in-memory limit of ExactlyOnce dedup. This keeps the honesty concern met.
  - Options considered: Document the limit; Persist the delivered-id history
  - Trade-offs: Documenting is cheap; persisting adds a write on every delivery
  - Needs sign-off from: Brian, Resolving Architecture (owner and maintainer)
  - Open questions: Is persisted dedup wanted before 2.0?

## Improve the Review

- Should the 'never silently discarded' NFR hold for AtMostOnce channels, which have no persistence directory?
- Is the one-second sleep in the hand-over test acceptable, or will it be replaced with a latch?
- Will the hand-over semantics be added to CORRECTNESS_SUITE.md so all seven ports verify them?
- Is a short duplicate window after a crash, caused by ack-after-forward, acceptable for ExactlyOnce?
- Do any callers rely on the old completed() behaviour of ratcheting the route even when the next stage refused?
- Test results or CI run for the three new tests and the full suite after this change
- Whether CORRECTNESS_SUITE.md was updated or already specifies behaviour on a refused hand-over
- How the other six ports currently handle a refused hand-over
- Whether 1M5 or service-bus depend on the return value or side effects of MessageBus.completed
- Intended dead-letter behaviour for AtMostOnce channels

## Notes

- The cached model says fullReviewRequired=true, while the current readiness assessment says false. Implementation evidence is now present, but its code snapshot predates this change.
- The two review markdown files in this change set are generated analyses, not accepted decisions. Remaining loss paths they list (DropOldest persisted files, AtMostOnce dead-letter, callbacks map, Block without timeout) are not addressed here.
- No build or test execution evidence was supplied for this change, so confidence in its correctness is limited.

---

Meridian recommends. The customer organization decides.

