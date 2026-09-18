# ADR-005: Consumer group per operation vs. one group

## Context

`message-processor` consumes four request topics. Kafka consumer groups are the unit of
independent scaling and independent lag monitoring — two listeners in the same group compete
for the same partitions; listeners in different groups don't.

## Decision

One consumer group per operation: `processor-create`, `processor-update`, `processor-delete`,
`processor-read`, each with its own `ConsumerFactory` and
`ConcurrentKafkaListenerContainerFactory` (`KafkaConsumerConfig`). Concurrency per group
defaults to the partition count (6) and is overridable per operation via
`messaging.kafka.consumer.concurrency`.

## Alternatives considered

**One consumer group for all four topics.** Fewer moving parts to operate — one group, one lag
number, one set of consumer offsets to reason about. Rejected because it collapses
`message-processor` scaling into an all-or-nothing knob: write-heavy load (Create/Update) and
read-heavy load (Read) would have to scale together even though they hit different resources
(DB writes vs. a request-reply round trip) and have different latency profiles. It also means
one topic's consumer lag is invisible on its own — it's buried inside the group's aggregate lag
across all four topics.

## Consequences

- Write-heavy and read-heavy paths scale and are monitored independently — this is the entire
  point, and the reason it was chosen despite the operational cost below.
- **The cost is real: four consumer groups to operate instead of one.** Four lag metrics to
  watch, four sets of consumer offsets, four places a misconfiguration (wrong `group.id`, wrong
  concurrency) can hide. For four operations this is manageable; it would need reconsidering if
  the operation count grew much further.
- Concurrency per group is config, not a hardcoded constant, specifically so it can be tuned
  per operation without a code change if one operation's load profile diverges from the others.
