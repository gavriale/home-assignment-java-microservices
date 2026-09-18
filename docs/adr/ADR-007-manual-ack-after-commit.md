# ADR-007: Manual ack after DB commit — the at-least-once delivery contract

## Context

Kafka's auto-commit periodically commits offsets on a timer, independent of whether the record
was actually processed successfully. If MS-2 crashes after auto-commit but before the DB write
lands, the record is silently lost — never redelivered, never applied. The brief's requirement
that MS-2 "performs the matching DB operation" means the DB write is the thing that must define
"processed," not the poll loop returning a batch.

## Decision

`enable.auto.commit=false` on every consumer, `AckMode.RECORD` on every listener container
(`KafkaConsumerConfig`). Combined with Spring Kafka's listener semantics, the offset for a
record is committed only after its `@KafkaListener` method returns normally — which, since
`JpaMessageRepositoryAdapter`'s methods are `@Transactional`, is only after the DB transaction
for that record has already committed. No manual `Acknowledgment` object is handled directly;
`AckMode.RECORD` gets the same "commit after successful return" behavior declaratively.

## Alternatives considered

**Auto-commit.** Rejected outright per the context above — it decouples "offset committed" from
"work actually done," which is the exact failure mode that loses records silently on a crash.

**`AckMode.BATCH`** (commit once per poll batch rather than per record). Slightly higher
throughput, but a single failure partway through a batch would either lose the successfully
processed records ahead of it (if the whole batch's offset commit is skipped) or reprocess them
unnecessarily — `RECORD` keeps the granularity matched to the transactional unit (one DB write
per record), which is what makes the crash-recovery story in "Consequences" below correct.

## Consequences

- **What happens when MS-2 crashes mid-batch:** every record whose DB transaction already
  committed has its offset committed and is not redelivered. The record being processed at
  crash time, and everything after it in the batch, was never offset-committed — on restart
  (or on a partition rebalance to another instance), the consumer resumes from the last
  committed offset and reprocesses from there.
- **What happens on redelivery:** the same record is applied again. This is safe because every
  operation is naturally idempotent (ADR-006) — reprocessing is a correctness-neutral,
  performance-only cost.
- This is Kafka's standard at-least-once contract, made explicit and correct rather than left to
  auto-commit's weaker, timer-driven guarantee. Exactly-once would need a transactional
  outbox/consumer-producer transaction spanning the DB and the offset commit — not pursued here;
  see the README's "what I'd add next" section for why.
