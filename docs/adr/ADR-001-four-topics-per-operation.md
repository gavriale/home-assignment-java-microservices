# ADR-001: Four topics per operation vs. one topic with an operation header

## Context

The brief requires MS-1 to publish each operation (Create, Update, Delete, Read) to its own
Kafka topic, and MS-2 to consume from those topics and perform the matching DB operation. That
is a hard requirement, not a design choice — but it has a real consequence worth naming: it
trades away total per-key ordering.

## Decision

Five request topics, one per operation (`messages.create.v1`, `messages.update.v1`,
`messages.delete.v1`, `messages.read.v1`), plus `messages.read.reply.v1` for the request-reply
response (ADR-003) and `messages.dlt.v1` as the shared dead letter topic. All are keyed by
`id` as a `String` (ADR-004).

## Alternatives considered

**One topic (`messages.v1`) with an `operation` header.** MS-2 would dispatch on the header
instead of the source topic. This gives total ordering per key: a Create and a subsequent
Delete for the same `id` are guaranteed to arrive in publish order, because same-key records on
one topic always land on the same partition and partitions are strictly ordered. It would also
mean one consumer group instead of four (see ADR-005), simpler operationally.

It was not chosen because the brief explicitly asks for a topic per operation
("Publishes each to its own Kafka topic (separate topic per operation)"). It also loses
per-operation scaling and lag monitoring — see ADR-005's tradeoff in the other direction.

## Consequences

- **Ordering is per-topic, not per-key-across-topics.** A `Delete` on `messages.delete.v1` can
  be consumed before a `Create` on `messages.create.v1` for the same `id`, because they are two
  independent partitions in two independent topics with no ordering relationship between them.
- Mitigated, not eliminated, by:
  - Every event carrying `occurredAt` (`Instant`) and `eventId` (`UUID`).
  - All DB operations being naturally idempotent and tolerant of out-of-order arrival —
    Create/Update are upserts, Delete is delete-if-exists (see ADR-006).
- Each operation gets its own consumer group, its own lag metric, and can be scaled
  independently (ADR-005) — a genuine benefit of the split, not just a constraint to work
  around.
