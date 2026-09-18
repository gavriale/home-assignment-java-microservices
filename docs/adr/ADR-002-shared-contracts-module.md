# ADR-002: Shared `contracts` module vs. duplicated schemas vs. schema registry

## Context

MS-1 produces events that MS-2 must be able to deserialize correctly. If the two services
define the wire format independently, they can drift silently — a field renamed on one side
is a runtime deserialization failure on the other, not a compile error.

## Decision

A third Maven module, `contracts`, owns the wire format: the sealed `MessageEvent` hierarchy
(`CreateRequested`, `UpdateRequested`, `DeleteRequested`, `ReadRequested`, `ReadReply`), topic
name constants (`Topics`), and header name constants (`Headers`). Both `message-api` and
`message-processor` depend on it. It contains **nothing else** — no Spring, no business logic,
no JPA — by design, so it can never become a dumping ground for shared utilities that would
recreate real coupling between the two services.

## Alternatives considered

**Duplicated schemas** — each service defines its own copy of the event records. Rejected:
this is exactly the drift scenario above, and for two services under one team's control in one
repo, there is no benefit to paying that cost.

**Schema registry (Avro/Protobuf via Confluent Schema Registry or similar)** — the production
answer. It enforces compatibility at publish time across service and even language boundaries,
independent of a shared JVM module, and supports schema evolution rules (backward/forward
compatibility) explicitly. Rejected here because it is another moving part (registry service,
serializer/deserializer config, schema IDs) with no payoff at this scale: two services, one
language, one repo, one team. This is the first thing to introduce if a third service or a
second language joins.

## Consequences

- A wire-format change is a single-module edit that both services pick up at their next build —
  the compiler catches drift instead of a runtime deserialization failure.
- This is real coupling: `message-api` and `message-processor` cannot be deployed against
  incompatible versions of `contracts` without risk. Acceptable because they are versioned,
  built, and (per the brief) delivered together from one repository.
- If a third consumer or a non-JVM service ever needs these events, that is the trigger to
  replace `contracts` with a schema registry — not before.
