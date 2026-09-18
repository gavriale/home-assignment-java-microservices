# ADR-004: Keying by `id` — ordering guarantees and their limits

## Context

Kafka guarantees ordered delivery only within a single partition. Which partition a record
lands on is determined by its key (absent a custom partitioner): same key → same partition,
different keys → no ordering relationship at all. "Scale out" (running multiple
`message-processor` instances) means multiple partitions are being consumed in parallel by
design — so without a deliberate key, two records for the same entity can be processed by two
different instances at the same time, in whatever order they happen to be scheduled.

## Decision

Every Kafka record — request and reply — is keyed by `id`, converted to a `String`. This is
set consistently by `KafkaMessagePublisher` in `message-api` for every operation.

## Alternatives considered

**No key (`null`) / round-robin partitioning.** Maximizes even load distribution across
partitions, but gives up ordering entirely: two records for the same `id` could be processed
concurrently by two different `message-processor` instances, racing each other into Postgres.
Rejected outright — this is the single highest-signal detail in the exercise, and silently
getting it wrong is the kind of bug that only shows up under concurrent load in production.

**A custom partitioner keyed on something other than `id`** (e.g. a tenant or shard id) was not
considered seriously: there is no coarser grouping in this domain that both distributes load
and preserves the ordering guarantee that actually matters, which is per-entity.

## Consequences

- Same `id` → same partition → ordered delivery **within one topic**, regardless of which
  `message-processor` instance ends up consuming that partition.
- This guarantees ordering *within* a topic, not *across* topics. Because the brief mandates
  four separate topics (ADR-001), a `Delete` for `id=7` published to `messages.delete.v1` can
  still be consumed before an `Update` for `id=7` published to `messages.update.v1` — keying
  cannot fix a cross-topic race, only a same-topic one. That gap is closed at the operation
  level instead, by making every DB operation idempotent and order-tolerant (ADR-006).
- 6 partitions per topic (configured, not hardcoded) is enough to demonstrate parallelism while
  staying easy to reason about in the demo; see ADR-005 for how consumer concurrency relates to
  this number.
