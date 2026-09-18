# ADR-003: Synchronous request-reply for Read vs. 202-and-poll vs. CQRS read model

## Context

Kafka is fire-and-forget: publishing a record gives no path back to the producer for a
response. The brief nonetheless requires Read to go through Kafka like the other three
operations, and a `GET` is expected to return data, not just an acknowledgement.

## Decision

Request-reply, using Spring Kafka's `ReplyingKafkaTemplate`:

- `message-api` publishes `ReadRequested` with `KafkaHeaders.REPLY_TOPIC` set to
  `messages.read.reply.v1` and a `KafkaHeaders.CORRELATION_ID`, then blocks on the returned
  future with a configurable timeout (default 3s).
- `message-processor`'s `@KafkaListener` for `messages.read.v1` **returns** a `ReadReply`
  value; Spring Kafka publishes it to the reply topic/partition carried on the inbound
  record's headers automatically — no `@SendTo` needed, since the destination is per-request.
- `message-api` maps the outcome to `200` (found), `404` (`MessageNotFoundException`), or
  `504` (`ReplyTimeoutException`) via the shared `ProblemDetail` advice (see CLAUDE.md §5.9).

**Scale-out subtlety:** if every `message-api` instance shared one consumer group on the reply
topic, a reply could land on whichever instance Kafka assigned that partition to — not
necessarily the instance actually waiting for it. `ReplyPartitionResolver` fixes this by having
each instance claim one partition of `messages.read.reply.v1` for its lifetime, derived by
hashing its container hostname mod the partition count, and setting
`KafkaHeaders.REPLY_PARTITION` on the request so the reply is written directly to that
partition.

## Alternatives considered

**202-and-poll.** `GET` returns `202` with a location to poll, client polls until the result is
ready. Rejected: more round trips for the caller, and it only defers the same fundamental
problem (something still has to correlate the eventual Kafka reply to the polling request) —
it doesn't remove request-reply, it just spreads it across more HTTP calls.

**A unique reply topic per `message-api` instance.** Simplest to reason about — no partition
hashing, no collision risk — but does not scale operationally: every instance creates and
tears down a topic, and topic count grows with instance count. Partition-per-instance on one
shared topic bounds resource growth by the topic's partition count instead.

**CQRS read model** (the production answer, not built here). MS-2 owns writes; a read model is
projected (e.g. into a separate table, cache, or search index) from the same events MS-2
already consumes, and MS-1 reads that model directly over a normal synchronous call —
no Kafka round trip on the read path at all.

## Consequences

- This reintroduces synchronous coupling over an inherently asynchronous transport: a Read now
  depends on `message-processor`'s and Kafka's availability and latency in a way Create/Update/
  Delete never do, and inherits the broker's availability characteristics on the read path.
  This is the reason it would not ship this way in production — CQRS is the answer there.
- The partition-hash approach to reply routing has a real, accepted collision risk: two
  instances could hash to the same partition. A production system would hand out partitions
  from a coordinator (e.g. a StatefulSet ordinal) instead. Not worth the extra moving part here,
  since `message-api` is not the service this exercise scales out in the demo (`message-processor`
  is — see README "Scaling out").
- Timeout is externalized (`messaging.kafka.reply.timeout`, default 3s), not hardcoded.
