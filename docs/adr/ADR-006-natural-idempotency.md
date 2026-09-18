# ADR-006: Natural idempotency vs. an event dedup table

## Context

Kafka delivery is at-least-once: a broker failover, a consumer restart before an offset commit,
or a retry after a transient error can all cause the same record to be delivered to
`message-processor` more than once. Combined with the cross-topic ordering gap from ADR-001/
ADR-004, operations must also tolerate arriving more than once and out of order.

## Decision

Every operation handler is naturally idempotent at the data-operation level:

- `CreateOperationHandler` and `UpdateOperationHandler` both call `MessageRepositoryPort.upsert`
  — applying either twice, or applying an Update before its Create arrives, converges to the
  same final row.
- `DeleteOperationHandler` deletes if the row exists and is a no-op otherwise — applying it
  twice, or applying it before the Create it logically follows, is safe.

No separate deduplication step exists; correctness comes from the operations themselves being
safe to repeat, not from filtering out repeats before they reach the operation.

## Alternatives considered

**A `processed_events` table keyed by `eventId`, checked (and inserted) inside the same
transaction as the write.** This is the stronger guarantee — true exactly-once application
semantics at the row level, independent of whether the underlying operation happens to be
naturally idempotent. Not adopted here: it is a real cost in write amplification (every
operation becomes two writes instead of one, in the same transaction) for marginal benefit,
given that all four operations in this domain are already naturally idempotent without it. It
would earn its cost the moment an operation is added that *isn't* naturally idempotent (an
increment, an append, anything non-commutative).

## Consequences

- Duplicate delivery of any operation is safe by construction — no special-casing needed in
  the handler, no coordination needed across `message-processor` instances.
- This only works because every current operation happens to be naturally idempotent. Adding a
  future operation that isn't (e.g. "append to history") would need either its own
  idempotency mechanism or would be the trigger to introduce the dedup table above for real.
- Manual ack after DB commit (ADR-007) plus this idempotency together define the full
  at-least-once contract: a crash between DB commit and offset commit causes redelivery, and
  redelivery is safe to apply again.
