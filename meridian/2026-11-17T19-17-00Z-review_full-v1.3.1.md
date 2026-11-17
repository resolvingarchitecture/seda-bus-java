---
service: REVIEW_FULL
project_version: 1.3.1
version_source: pom.xml
reviewed_at: 2026-11-17T19:17:00.485072644Z
architecture_model_version: 7
project_id: 10d346e8-480c-458e-ab59-8cc5595d6b7b
evaluation_id: 4eea1cfd-9191-4a19-bc14-22885b546c53
---

# Architecture Review

## Executive Summary

Review readiness is **HIGH_CONFIDENCE**. The review recorded 3 findings with 4 gaps (2 medium, 1 none).

Most significant:

- **Application — Observability** (medium): There are no counters or failure event hook; metrics hooks are on the roadmap only.
- **Application — Testability** (medium): One test class covers the engine and replay, and some tests are timing-flaky.
- **Infrastructure — Observability** (none): Not applicable beyond library counters.

## Not Applicable to a Library

Every component is a library, so checks about deploying and operating a service were not applied. They are listed so their absence from the findings is explained, not overlooked.

| Check | Not applied because |
|-------|---------------------|
| Infrastructure provisioning approach is unclear (RULE-INFRA-001) | A library has no infrastructure of its own. |
| Infrastructure-as-code signal is missing (RULE-INFRA-002) | A library has no infrastructure of its own. |
| Rollback or progressive delivery strategy is missing (RULE-INFRA-004) | A library is not deployed; consumers choose when to upgrade. |
| Kubernetes workload lacks deployment and health-check signals (RULE-INFRA-005) | A library is not a workload. |
| Capacity planning or autoscaling signal is missing (RULE-INFRA-006) | A library is not deployed or operated by its author; the host owns this. |
| Runtime protection signals are weak (RULE-INFRA-007) | A library is not deployed or operated by its author; the host owns this. |
| Scaling strategy is not evident (RULE-INFRA-008) | A library is not deployed or operated by its author; the host owns this. |
| Resilience topology signal is missing (RULE-INFRA-009) | Zones, regions, and disaster recovery are properties of the deployment, not of an in-process library. |
| Observability signal is missing (RULE-INFRA-010) | Operational telemetry (dashboards, alerts, ownership) belongs to the host. A library is expected to expose cheap counters and an event hook for failures, which the review judges separately. |
| Service-level objective signal is missing (RULE-INFRA-011) | A library has no service to set objectives for; the host does. |
| Internet-facing workload lacks edge protection signal (RULE-INFRA-012) | A library has no network surface; it trusts its in-process caller. |
| Network segmentation signal is missing (RULE-INFRA-013) | A library has no network surface; it trusts its in-process caller. |
| Secrets management posture is unclear (RULE-INFRA-014) | A library holds no secrets of its own; the host decides what goes into an envelope. |
| Operational runbook or incident response signal is missing (RULE-INFRA-016) | A library is not deployed or operated by its author; the host owns this. |
| Cost-management signal is missing (RULE-INFRA-017) | A library is not deployed or operated by its author; the host owns this. |
| Infrastructure operating model is under-described (RULE-INFRA-018) | A library is not deployed or operated by its author; the host owns this. |
| Missing integration observability signal (RULE-INT-007) | Operational telemetry (dashboards, alerts, ownership) belongs to the host. A library is expected to expose cheap counters and an event hook for failures, which the review judges separately. |
| Missing identity boundary signal (RULE-SECARCH-001) | A library has no network surface; it trusts its in-process caller. |
| Missing encryption-in-transit signal for sensitive data (RULE-SECARCH-004) | A library has no network surface; it trusts its in-process caller. |
| Missing audit logging signal for administrative access (RULE-SECARCH-005) | A library has no network surface; it trusts its in-process caller. |
| Missing secrets management signal (RULE-SECARCH-006) | A library holds no secrets of its own. |
| Missing edge protection signal for internet-facing workload (RULE-SECARCH-007) | A library has no network surface; it trusts its in-process caller. |
| Missing least-privilege access signal (RULE-SECARCH-008) | A library has no network surface; it trusts its in-process caller. |
| Stateful application server may block horizontal scaling (RULE-STACK-001) | A library is not deployed or operated by its author; the host owns this. |
| Serverless runtime may not fit long-running workloads (RULE-STACK-003) | A library is not deployed or operated by its author; the host owns this. |
| Framework-managed sessions lack an external session store (RULE-STACK-005) | A library has no sessions. |
| Container runtime lacks explicit health check signal (RULE-STACK-008) | A library is not deployed or operated by its author; the host owns this. |
| Kubernetes deployment lacks resource limit signal (RULE-STACK-009) | A library is not deployed or operated by its author; the host owns this. |
| Missing observability signal for scale-up stage (RULE-STAGE-002) | Operational telemetry (dashboards, alerts, ownership) belongs to the host. A library is expected to expose cheap counters and an event hook for failures, which the review judges separately. |
| Missing deployment maturity signal for later-stage architecture (RULE-STAGE-003) | A library is not deployed or operated by its author; the host owns this. |
| Single database node risk under scaling pressure (RULE-SCALE-001) | A library is not deployed or operated by its author; the host owns this. |
| Missing horizontal scaling signal at scale-up (RULE-SCALE-004) | A library is not deployed or operated by its author; the host owns this. |
| Stateful application sessions can block horizontal scaling (RULE-SCALE-005) | A library has no sessions. |
| Missing read-replica strategy for read-heavy scale-up workload (RULE-SCALE-008) | A library is not deployed or operated by its author; the host owns this. |
| Missing backup and restore signal (RULE-DATA-004) | Files a library writes live in the host's storage; backing them up is the host's concern. |

## Overall Quality

No overall quality assessment was provided.

## Classification


| Domain | Confidence | Evidence |
|--------|------------|----------|
| Application | 0.80 | Component boundaries: SEDABus, SEDAMessageChannel, WorkerThreadPool, Backpressure; Delivery levels, retry and dead-letter, persistence; NFRs for reliability, performance and portability |
| Integration | 0.70 | MessageBus and Envelope contract from ra-common; Routing slip; Back-pressure failing send() |
| Data | 0.20 | Persisted channel files; deadLetter.json |
| Infrastructure | 0.30 | Java 11 target; jitpack distribution |
| Security | 0.30 | Trusts the in-process caller; Unencrypted local persistence |
| Enterprise | 0.60 | Goals, governance, roadmap in TODO.md, seven-language ports |

## Review Readiness

- **Status:** HIGH_CONFIDENCE
- **Guidance:** The following selected architecture domains are ready for high-fidelity review: APPLICATION, INTEGRATION, INFRASTRUCTURE, ENTERPRISE.

### Domain Readiness

| Domain | Present | High-fidelity ready | Confidence |
|--------|---------|---------------------|------------|
| APPLICATION | true | true | 0.70 |
| INTEGRATION | true | true | 0.95 |
| DATA | false | false | 0.00 |
| INFRASTRUCTURE | true | true | 0.70 |
| SECURITY | false | false | 0.00 |
| ENTERPRISE | true | true | 0.70 |

## Stakeholders

- Brian Taylor (owner/maintainer) — source: explicit
  - Concern: Back-pressure: no stage may monopolise the shared pool or grow unbounded
  - Concern: Small, broker-less design with one dependency
  - Concern: Keeping the seven language ports in lockstep
- service-bus maintainers — source: explicit
  - Concern: No message loss on crash for AtLeastOnce and ExactlyOnce channels
- 1m5-desktop-java maintainers — source: explicit
  - Concern: Low dependency footprint
  - Concern: Java 11 compatibility

## Quality Attribute Priorities

No quality attribute ranking was provided.

## Domain Reviews

### Application

#### seda-bus library

**Observability** — _medium_

There are no counters or failure event hook; metrics hooks are on the roadmap only. Logging frameworks, dashboards and tracing do not apply to a library.

Gaps:

- No queue depth, retry or dead-letter counters
- No failure callback

**Testability** — _medium_

One test class covers the engine and replay, and some tests are timing-flaky. A latch and injected-clock fix is decided, but there is no crash-injection testing.

Gaps:

- Crash mid-write and replay tests
- Injectable file system

### Integration

#### Host ↔ seda-bus via ra.common.messaging.MessageBus / Envelope / DynamicRoutingSlip

Reviewed; no findings were recorded for this component.

### Data

No data-specific findings were produced from the supplied evidence.

### Infrastructure

#### Build and distribution of the seda-bus library

**Observability** — _none_

Not applicable beyond library counters.

### Security

No security-specific findings were produced from the supplied evidence.

### Enterprise

#### RA seda-bus family (seven language ports)

Reviewed; no findings were recorded for this component.

## Overall Gaps

No overall architecture gaps were identified in the structured review output.

## Recommended Additional Context

- Provide quality attribute priorities such as security, availability, scalability, performance, maintainability, and cost.
- Provide known risks, unresolved architecture concerns, or current technical debt so gaps can be assessed more precisely.
- Provide success criteria or acceptance thresholds for determining architecture quality.
- Decision-grade architecture review requires implementation validation because the review must compare architecture intent against evidence of what is actually implemented or proven.

Please provide:
- source, configuration, deployment, test, runtime, monitoring, proof-of-concept, incident, or traceability evidence,
- which decisions have been implemented,
- how they were verified,
- what evidence shows the implementation satisfies the intended architecture and quality targets.

This evidence comes from the Implementation Model — the backend's scan of a repository
Source (its init scan and later changesets) — not from the Architecture Model, which holds
intent only. Register the repository as a Source in meridian-web and run its init so the
Implementation Model has documents and an identified-instance comparison against the
Architecture Model. Example of the underlying Implementation Model shape:

{
  "contextId": "11111111-1111-1111-1111-111111111111",
  "documents": [
    {
      "id": "order-fulfillment-event-test",
      "title": "OrderFulfillmentEventTest.java",
      "typeHint": "CODEBASE",
      "content": [
        {
          "contentType": "TEXT",
          "encoding": "UTF8",
          "data": "Order fulfillment event flow is implemented in OrderService. Integration tests verify event publication and retry behavior. Kubernetes deployment includes readiness checks, CPU/memory limits, and autoscaling from 2 to 10 replicas."
        }
      ]
    }
  ]
}

- The following selected architecture domains are ready for high-fidelity review: APPLICATION, INTEGRATION, INFRASTRUCTURE, ENTERPRISE.
- Provide: Application component boundaries and responsibilities
- Provide: Runtime dependencies and important request flows
- Provide: Application-level non-functional requirements
- Provide: Major application design decisions and trade-offs
- Provide: API contracts, event schemas, or integration diagrams
- Provide: Producer and consumer ownership
- Provide: Inter-system SLAs, retry behavior, and idempotency requirements
- Provide: Deployment topology and runtime environment
- Provide: Cloud, network, compute, and storage decisions
- Provide: CI/CD, infrastructure-as-code, scaling, and disaster recovery expectations
- Provide: Business goals and success metrics
- Provide: Enterprise architecture principles and standards
- Provide: Capability map, roadmap, governance model, and decision rights
- Validation missing coverage: rubric:application-architecture
- Validation missing coverage: rubric:infrastructure-architecture
- Validation missing coverage: rubric:enterprise-architecture
- Validation warning: One or more activated architecture rubrics were not represented in the draft

## Rules Checked

105 rules apply to this system: 8 were checked against the model and 97 were judged in this review. 182 do not apply to a system of this kind, and 21 wait on context that has not been confirmed.

### Findings (59)

| Rule | Severity | Evidence | Recommendation |
|------|----------|----------|----------------|
| Component dependencies are acyclic, or each cycle is a recorded decision (RULE-APP-009) | MEDIUM (HIGH if the cycle spans layers) | dependency cycle: sedabus → sedamessagechannel → workerthreadpool → sedabus |  |
| Goals are measurable and owned (RULE-ENT-020) | MEDIUM | goal-c3f7f0ad "Provide a small, dependency-light, in-process SEDA bus th..." lacks owner; goal-4ee7af6d "Stay consumable by Java 11 downstreams and keep behavior ..." lacks owner |  |
| Layers or packages and the allowed dependency direction are stated (RULE-APP-001) | MEDIUM | Components are named but no packages or allowed dependency direction are stated, and a cycle exists. | Document the package layout and allowed dependency direction. |
| Every component has one stated responsibility and an owner (RULE-APP-008) | MEDIUM | SEDAMessageChannel is described as queue, delivery level, retry/dead-letter and persistence. | State one responsibility per component and name its owner. |
| The public surface is explicit and the rest is internal (RULE-APP-010) | HIGH foundational system; MEDIUM otherwise | Only MessageBus is named; exported packages and internal visibility are not listed. | List the public API and hide the rest, for example with module-info exports. |
| Components are cohesive and of reasonable size (RULE-APP-012) | MEDIUM | SEDAMessageChannel combines queueing, delivery, retry, dead-letter and persistence; its size is not measured. | Extract persistence and retry/dead-letter policy into collaborators. |
| The public contract is documented (RULE-APP-020) | HIGH foundational system; MEDIUM otherwise | No per-operation inputs, errors, nullability, ordering or thread-safety are documented. | Write the Javadoc contract for each public operation. |
| A versioning and compatibility policy exists (RULE-APP-021) | HIGH foundational; MEDIUM otherwise | No versioning or deprecation policy is recorded for a library with dependents. | Adopt SemVer, define what counts as breaking, and set a deprecation window. |
| Failures are reported in one consistent way (RULE-APP-022) | MEDIUM | send() 'fails'; the form (false or exception) is unstated, and dead-lettering is not reported to the producer. | Define one failure channel and apply it everywhere. |
| Configuration is documented, validated and safe by default (RULE-APP-024) | MEDIUM | Static per-stage settings (concurrency, queue size, maxAttempts) have no documented defaults, ranges or validation. | Document defaults and ranges, and reject invalid values. |
| Lifecycle is defined, including what happens to work in flight (RULE-APP-025) | HIGH if it holds work; MEDIUM otherwise | Start/stop/close and the fate of queued and in-flight envelopes at shutdown are not stated. | Define idempotent lifecycle and the drain or persist behaviour on shutdown. |
| Which calls may block is stated, and blocking cannot exhaust the worker pool (RULE-APP-032) | HIGH | Handler and callback threads are unstated, and a blocking handler holds shared-pool threads. | Document handler threads and forbid or guard blocking calls. |
| Ordering guarantees are stated, and so is where they stop (RULE-APP-034) | MEDIUM | Only in-order replay is stated; ordering under concurrency above one and under retries is unstated. | State per-channel ordering and what breaks it. |
| Long operations have timeouts and can be cancelled (RULE-APP-035) | MEDIUM | No timeouts or cancellation are described for drain, persistence or callbacks. | Add timeouts or cancellation for long operations and define the outcome. |
| Failure modes are listed for each operation (RULE-APP-040) | HIGH foundational system; MEDIUM otherwise | Failure handling is described globally, not per public operation; persistence I/O failure is not covered. | Tabulate failure modes, caller view, loss and retry safety for each operation. |
| No failure is silent (RULE-APP-041) | HIGH | There is no failure event hook; how persistence and dead-letter write errors surface is undescribed. | Add a failure listener and surface I/O errors. |
| Every external or handler call has a timeout (RULE-APP-045) | MEDIUM | Calls into user handlers have no time limit. | Add a per-channel handler timeout that counts as a nack. |
| On-disk and wire formats are documented and versioned (RULE-APP-051) | MEDIUM | The channel files and deadLetter.json formats are undocumented and carry no version. | Document the persisted format, add a version field, and plan migration. |
| Deduplication state is as durable as the guarantee it supports (RULE-APP-052) | HIGH | Whether the bounded ExactlyOnce id history is persisted across crashes is not stated. | Persist dedup ids atomically with delivery state, or narrow the claim. |
| Performance claims have a benchmark (RULE-APP-061) | MEDIUM | The lock-free hot path and no-polling claims have no benchmark. | Add a JMH benchmark and record the results. |
| Failure paths and concurrency are tested, and time and I/O can be faked (RULE-APP-062) | HIGH foundational system; MEDIUM otherwise | One test class, flaky timing tests, and no crash-mid-write or injectable file-system tests. | Add failure-injection and concurrency tests with an injected clock and file system. |
| Observability is proportionate to the class (RULE-APP-063) | MEDIUM | No counters or failure hook; metrics hooks appear only on the roadmap. | Expose depth, retry and dead-letter counters and a failure listener. |
| Compatibility is checked automatically for a library with dependents (RULE-APP-065) | MEDIUM | No automated API compatibility check is evidenced. | Add japicmp to the build against the previous release. |
| Change is easy where change is expected (RULE-APP-066) | LOW | The adaptive controller is deferred to 2.0 with no described seam. | Define a stage-controller interface now, with a static implementation. |
| Dependencies are few, pinned and scanned (RULE-APP-073) | MEDIUM | One dependency, pinned at 1.2.0, but vulnerability scanning is not evidenced. | Add a dependency scan (OWASP dependency-check) to the build. |
| Structural decisions are recorded with their alternatives (RULE-APP-004) | LOW | Decisions record rationale but name no rejected alternatives. | Add the alternatives considered to the structural ADRs. |
| The build is reproducible and the target is tested (RULE-APP-080) | MEDIUM | Java 11 release and versions are fixed, but no CI exercises the Java 11 runtime. | Run the build and tests on JDK 11 in CI. |
| Releases are traceable (RULE-APP-082) | LOW | Not published; no changelog or tag process is evidenced. | Tag releases and keep a CHANGELOG. |
| Each integration has an API contract and a version (RULE-INT-005) | MEDIUM | The MessageBus contract has no written operations or errors, and seda-bus's own version is unstated. | Document the contract and version seda-bus releases. |
| Each external dependency has an owner (RULE-INT-006) | MEDIUM | The owner of ra-common is not named. | Name the ra-common owner and the downstream contacts. |
| Producers and consumers of every integration are identified (RULE-INT-020) | MEDIUM | Consumers are named, but how they are told of changes is unstated. | Define change notification, for example release notes or an issue tracker. |
| Contracts are machine-readable and tested (RULE-INT-021) | HIGH foundational; MEDIUM otherwise | A Java interface exists, but no conformance suite tests implementations against it. | Create MessageBus conformance tests. |
| Change follows compatibility rules (RULE-INT-022) | HIGH foundational; MEDIUM otherwise | No compatibility or deprecation rules are recorded. | Adopt additive-only changes with a deprecation window. |
| Delivery semantics and ordering are stated for each channel (RULE-INT-040) | HIGH | Delivery level is set per channel, but ordering per channel is not stated. | State the ordering guarantee for each delivery level. |
| Backlog and lag are visible and bounded (RULE-INT-043) | MEDIUM | Queues are bounded, but depth and lag are not exposed. | Expose depth and lag counters. |
| The host contract is minimal and stable (RULE-INT-080) | HIGH foundational system | The host contract is named (MessageBus, Envelope) but not versioned, and the handler interface is unspecified. | Enumerate and version the host-facing interfaces. |
| Sibling implementations are held in step by a conformance suite (RULE-INT-082) | HIGH | Seven ports are kept in lockstep by governance only; no shared test vectors exist. | Publish cross-language conformance test vectors. |
| Handler failure is part of the contract (RULE-INT-083) | HIGH | Only nack→retry is defined; throw, block and re-entrant send are undefined. | Specify handler throw, block and re-entrancy semantics. |
| Dependencies are scanned and listed (RULE-SECARCH-053) | HIGH | No SBOM or build-time vulnerability scan is evidenced. | Generate a CycloneDX SBOM and scan on each build. |
| An incident response plan exists and has been exercised (RULE-SECARCH-072) | MEDIUM | No incident response plan is evidenced. | Add a lightweight SECURITY.md response procedure. |
| Vulnerabilities are disclosed and patched on a schedule (RULE-SECARCH-073) | MEDIUM | No disclosure channel or fix-time target is recorded. | Publish a SECURITY.md with a contact and fix SLA. |
| Releases are signed and their integrity can be checked (RULE-SECARCH-080) | HIGH for a foundational system | Distributed through jitpack with no signed artifacts. | Publish signed artifacts to Maven Central. |
| Builds and releases are automated (RULE-INFRA-003) | MEDIUM | No build, test or release pipeline is evidenced. | Add CI (for example GitHub Actions) for build, test and release. |
| Supported runtime versions are stated and tested (RULE-INFRA-090) | MEDIUM | Java 11 is stated, but no build exercises it. | Test on JDK 11 and the latest LTS in CI. |
| The resource footprint is documented and configurable (RULE-INFRA-091) | MEDIUM | Pool threads, queue memory and disk use are not documented. | Document the footprint defaults and how to configure them. |
| Business capabilities are identified and mapped (RULE-ENT-001) | MEDIUM | The capability is implied only by scope notes; there is no explicit capability mapping. | Name the in-process staged-messaging capability and map components to it. |
| A roadmap sequences the work and its dependencies (RULE-ENT-022) | LOW | Roadmap items in TODO.md are listed without ordering or dependencies. | Sequence the items, for example metrics hooks before the adaptive controller. |
| Build, buy or reuse is a decision for each major capability (RULE-ENT-023) | MEDIUM | No record of why the bus is built rather than using an existing library. | Record the build rationale and the alternatives considered. |
| Architecture is reviewed on a cadence (RULE-ENT-032) | LOW | No review cadence or triggers are stated. | Review at each minor release and on cross-port changes. |
| Decommissioning and lifecycle are planned (RULE-ENT-014) | LOW | No retirement plan, and the fate of persisted channel files is unstated. | Note the deprecation path and how to clean up persisted data. |
| Shared components have a lifecycle and versioning (RULE-ENT-041) | MEDIUM | Not published; there is no release process or deprecation path. | Define the release process and deprecation path. |
| Implementations in several languages are kept in step (RULE-ENT-042) | MEDIUM | Lockstep is a governance rule only, with no shared spec or drift detection. | Write a shared behaviour spec with test vectors. |
| Every component has an owning team or person and a support path (RULE-ENT-050) | MEDIUM | There is an owner, but no support path is stated. | Name the issue tracker and support channel. |
| Key-person risk is identified with a mitigation (RULE-ENT-051) | MEDIUM | A single owner holds all decisions across seven ports; the risk is unregistered. | Register the key-person risk and name a backup maintainer. |
| Licensing and intellectual-property obligations are understood (RULE-ENT-061) | MEDIUM | MIT is recorded; the ra-common licence and its compatibility are not. | Record the ra-common licence. |
| Complexity does not exceed team capacity (RULE-ORG-001) | HIGH | One maintainer keeps seven language ports in lockstep. | Reduce lockstep scope or add maintainers and automation. |
| Investment matches the stage (RULE-ENT-080) | MEDIUM | At a mature stage, the hard-to-change items (versioning, on-disk format, publication) are still undone. | Fix the API and format versioning before 2.0. |
| The path to the next stage is planned (RULE-ENT-082) | LOW | The 2.0 items lack ordering and triggers. | State the 2.0 entry criteria and their order. |
| The project can continue if its owner is unavailable (RULE-ENT-090) | HIGH if one person owns everything | A single owner approves everything; there is no documented release process and distribution is jitpack-only. | Document build and release steps and grant a second maintainer access. |

### Accepted (11)

| Rule | Severity | Evidence | Recommendation |
|------|----------|----------|----------------|
| Every queue, buffer, cache and map is bounded, with a stated policy at the limit (RULE-APP-033) | HIGH | Queues are bounded and fail send(); the unbounded deadLetter.json is a registered, accepted risk with rotation on the roadmap. |  |
| Retry is bounded, backed off, and safe (RULE-APP-042) | HIGH | Retry is bounded by maxAttempts; the lack of backoff is an accepted decision pending a backoff policy. |  |
| The trust boundary is stated and input is validated at it (RULE-APP-070) | MEDIUM (HIGH for a service) | Decision: the library trusts its in-process caller; the host is responsible for filesystem permissions. |  |
| Sensitive data is not kept or logged in clear unless that is a decision (RULE-APP-072) | MEDIUM | Clear-text persistence is a recorded decision; the host must not put secrets in envelopes. |  |
| Consumers are idempotent (RULE-INT-004) | HIGH | Decision: AtLeastOnce replay requires idempotent consumers; recorded as a risk. |  |
| Exactly-once claims hold end to end (RULE-INT-044) | HIGH | The bounded dedup window letting old duplicates through is a recorded, accepted risk; durability is covered by APP-052. |  |
| Each third-party dependency has an assumed service level, an exit plan and a failure behaviour (RULE-INT-060) | MEDIUM | Decision: consume through jitpack until published; the jitpack-only dependency is a registered risk. |  |
| Upstream types in the public API are a recorded decision (RULE-INT-081) | MEDIUM | Decisions make the ra-common MessageBus, Envelope and routing slip the contract, with changes made in lockstep. |  |
| Trust boundaries and data flows are drawn (RULE-SECARCH-040) | MEDIUM | The decision states the caller is trusted and that envelopes go to local files under host-managed permissions. |  |
| Security requirements are quality attributes with tests (RULE-SECARCH-042) | MEDIUM | The decision records no threat model beyond trusting the caller; sensitivity is none. |  |
| The library never logs or persists secrets it was not given to keep (RULE-SECARCH-102) | MEDIUM | Clear-text envelope persistence is a recorded decision; keeping secrets out of envelopes is the host's responsibility. |  |

### Unverified (13)

| Rule | Severity | Evidence | Recommendation |
|------|----------|----------|----------------|
| Delivery guarantees match the implementation (RULE-APP-044) | HIGH | No implementation scan to confirm persist-before-enqueue and ack-after-next-hop. |  |
| Resources are released on every path (RULE-APP-047) | MEDIUM | No code evidence for releasing file, thread and permit resources. |  |
| Logging is consistent and safe (RULE-APP-064) | LOW | No logging evidence was supplied. |  |
| Documentation matches the code (RULE-APP-081) | HIGH | No implementation model was supplied to compare with the documentation. |  |
| Boundary leakage (RULE-APP-090) | HIGH | No code evidence on persistence types leaking into the API. |  |
| Swallowed errors (RULE-APP-092) | MEDIUM | No code evidence. |  |
| Hidden global state (RULE-APP-093) | MEDIUM | No code evidence on static or singleton state. |  |
| Busy waiting (RULE-APP-094) | LOW | Event-driven drain is the design intent; the implementation was not scanned. |  |
| Premature or speculative abstraction (RULE-APP-095) | LOW | No code evidence. |  |
| A correlation id travels across every hop (RULE-INT-070) | MEDIUM | The Envelope id is implied by dedup, but its propagation into failure reports is undescribed. |  |
| Defaults are safe (RULE-SECARCH-043) | MEDIUM | Default file location and permissions of the persisted files are not stated. |  |
| Cost, funding and total cost of ownership assumptions are stated (RULE-ENT-024) | MEDIUM | Largely not applicable to an MIT in-process library with no run cost; funding is not recorded. |  |
| Skills cover the chosen stack (RULE-ENT-071) | MEDIUM | No skills evidence beyond the owner. |  |

### Passed (22)

- Each quality attribute that matters has a target and a way to verify it (RULE-APP-060)
- Goals are traced to decisions and components (RULE-ENT-021)
- Stakeholders, their concerns and the escalation path are identified (RULE-ENT-007)
- Decisions are logged with status and decider (RULE-ENT-030)
- Enterprise standards or a technology radar are referenced (RULE-ENT-004)
- A risk register has an owner and a status for each risk (RULE-ENT-060)
- No component is a hub for everything (RULE-APP-011)
- Overload is handled deliberately (RULE-APP-046)
- Each integration has an explicit boundary (RULE-INT-001)
- Message integration has retry and dead-letter handling (RULE-INT-003)
- The identity boundary is stated (RULE-SECARCH-001)
- Third-party code is vetted (RULE-SECARCH-082)
- Architecture principles are documented (RULE-ENT-003)
- A roadmap or target state is documented (RULE-ENT-005)
- Decision rights and a governance model exist (RULE-ENT-006)
- Stakeholder conflicts and priorities are resolved and recorded (RULE-ENT-033)
- Technology diversity is covered by standards (RULE-ENT-010)
- The architecture style has an enterprise rationale (RULE-ENT-015)
- Shared components are reused rather than rebuilt (RULE-ENT-040)
- Ownership is evident (RULE-ENT-008)
- Decision traceability exists at a later stage (RULE-STAGE-004)
- Technical debt is tracked and paid down (RULE-ENT-081)

### Waiting on context (21)

These rules were not judged: what decides whether they apply is not confirmed yet.

| Confirm | Rules it settles |
|---------|------------------|
| has a build pipeline | 4 |
| holds secrets | 4 |
| reads stored or received data | 4 |
| multi-threaded | 3 |
| dead-letter | 2 |
| publishes events | 2 |
| has users or identities | 1 |
| multi-step workflows | 1 |
| uses encryption | 1 |


