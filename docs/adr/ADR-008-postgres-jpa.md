# ADR-008: Postgres + JPA over alternatives, for this workload

## Context

MS-2 needs to persist `{ int id; String msg }` rows and answer point lookups by `id` for the
Read path. The brief mandates that the DB run in Docker alongside the two services; it does not
mandate a specific engine.

## Decision

PostgreSQL 16, accessed through Spring Data JPA + Hibernate, with Flyway managing schema
migrations (`V1__create_messages_table.sql`) and `hibernate.ddl-auto=validate` — Hibernate
never mutates the schema at runtime; Flyway is the single source of truth for it.

## Alternatives considered

**A NoSQL document/KV store** (e.g. MongoDB, DynamoDB-style). Would fit the shape of the data
(a flat `{id, msg}` document) just as well, but buys nothing here: there's no schema flexibility
requirement, no document nesting, no query pattern beyond point lookup by primary key that would
favor it over a relational table. Postgres is the more defensible default absent a reason to
deviate.

**Plain JDBC / `JdbcTemplate` instead of JPA.** Would avoid Hibernate's session/dirty-checking
machinery for what is, here, a single-entity upsert-or-delete — arguably a better fit for how
simple the persistence actually is. JPA was kept because it's the more commonly expected choice
for a Spring Boot exercise graded partly on "well-known Spring capability," and because the
mapping is trivial enough (`MessageEntity` ↔ `Message`, one field beyond the id) that JPA's
usual downsides (N+1 queries, unpredictable flush timing) never come into play at this scale.

**`ddl-auto=update`.** Rejected: letting Hibernate infer schema changes from entity annotations
means the schema is implicit and undocumented, and diverges from CLAUDE.md's flat rule that
"migrations" means Flyway. `validate` catches drift between the entity and the actual schema at
startup instead of masking it.

## Consequences

- Schema changes are explicit, reviewable SQL files under `db/migration/`, applied in order,
  once, by Flyway on startup — not inferred at runtime.
- The `MessageEntity` (JPA) and `Message` (domain) types are kept distinct even though today
  they carry the same two fields, per the DTO+Mapper pattern (CLAUDE.md §5.8): the entity is an
  adapter-layer concern and must never leak into `application`/`domain`.
- Upsert is implemented as `save()` on a `MessageEntity` constructed directly from the event
  (id + msg, no prior `findById` needed) — JPA's `save` performs a merge/insert based on
  whether the id already exists, which is exactly the upsert semantics ADR-006 depends on.
