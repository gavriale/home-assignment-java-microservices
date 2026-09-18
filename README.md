# Home Assignment: Java Microservices

A multi-process message-passing mechanism over Kafka, split into two Spring Boot
microservices:

- **message-api** (MS-1) — REST endpoints for Create/Update/Delete/Read over
  `{ int id; String msg; }`. Publishes each request to its own Kafka topic.
- **message-processor** (MS-2) — consumes those topics and performs the matching
  operation against PostgreSQL.

See `CLAUDE.md` for the full design brief and `docs/adr/` for why each decision was made.

## Prerequisites

- Docker Desktop (with Compose v2)

Nothing else is required on the host — Java, Maven, Kafka and Postgres all run inside
containers.

## Run it

```bash
cd docker
docker compose up --build
```

This starts, in order (via `depends_on: condition: service_healthy`):

1. `kafka` — single-node Kafka in KRaft mode (no ZooKeeper)
2. `postgres` — PostgreSQL 16, schema created by Flyway on `message-processor` startup
3. `message-api` — REST edge, port `8081`
4. `message-processor` — Kafka consumer + DB writer, port `8082` (not published to the host;
   see [Scaling out](#scaling-out))
5. `kafka-ui` — browser UI over the Kafka cluster, port `8080`

First build downloads Maven dependencies and base images, so expect a few minutes. Subsequent
runs are fast — Docker layer caching skips unchanged dependency layers.

## Swagger / API docs

Once `message-api` is up:

- Swagger UI: **http://localhost:8081/swagger-ui.html**
- Raw OpenAPI spec: http://localhost:8081/v3/api-docs

Use Swagger UI to exercise all four endpoints interactively — no separate Postman collection
is needed, but every example below also works as a plain curl command.

## API examples

All requests are keyed by `id` in Kafka, so operations on the same `id` are processed in order
within their own topic (see ADR-004 for what this does and doesn't guarantee).

**Create**

```bash
curl -i -X POST http://localhost:8081/api/v1/messages \
  -H "Content-Type: application/json" \
  -d '{"id": 1, "msg": "hello"}'
```

`202 Accepted` — the request was published, not yet applied. Body includes the Kafka
`eventId` and the correlation id.

**Update**

```bash
curl -i -X PUT http://localhost:8081/api/v1/messages/1 \
  -H "Content-Type: application/json" \
  -d '{"msg": "hello again"}'
```

**Read**

```bash
curl -i http://localhost:8081/api/v1/messages/1
```

This one *does* block — it's a synchronous Kafka request-reply (ADR-003), so it returns `200`
with the current row, `404` if the id was never created, or `504` if MS-2 doesn't reply within
3 seconds (configurable, see `message-api/src/main/resources/application.yml`).

**Delete**

```bash
curl -i -X DELETE http://localhost:8081/api/v1/messages/1
```

All four operations answer `202 Accepted` except Read, which answers synchronously as above.
Validation failures (e.g. a blank `msg`, a non-positive `id`) return `400` with an RFC 7807
`ProblemDetail` body — try it:

```bash
curl -i -X POST http://localhost:8081/api/v1/messages \
  -H "Content-Type: application/json" \
  -d '{"id": -1, "msg": ""}'
```

## Watching it work

- **Kafka UI** (http://localhost:8080) — browse the six topics
  (`messages.create.v1`, `messages.update.v1`, `messages.delete.v1`, `messages.read.v1`,
  `messages.read.reply.v1`, `messages.dlt.v1`), inspect partitions, keys and offsets, and watch
  consumer group lag per operation (`processor-create`, `processor-update`, `processor-delete`,
  `processor-read`).
- **Logs** — every log line carries the correlation id and message id
  (`docker compose logs -f message-api message-processor`). Boundary crossings are logged at
  `INFO` with partition/offset; payloads only at `DEBUG`.
- **Actuator** — `curl http://localhost:8081/actuator/health`,
  `.../actuator/metrics`, `.../actuator/prometheus` (same on `:8082` from inside the Docker
  network, or via `docker compose exec message-processor curl localhost:8082/actuator/health`
  since that port isn't published to the host — see [Scaling out](#scaling-out)).

## Scaling out

`message-processor` is the service this exercise scales — it's stateless Kafka consumers
writing to a shared Postgres instance, so more instances means more parallel consumption up to
the partition count (6 per topic):

```bash
docker compose up --scale message-processor=3 -d
docker compose logs -f message-processor
```

Watch the logs for Kafka's consumer group rebalance: each of the four consumer groups
(`processor-create`, `-update`, `-delete`, `-read`) redistributes its 6 partitions across the
now-3 instances. Because `message-processor` has no fixed host port mapping in
`docker-compose.yml` (only `expose`), multiple instances can coexist without a port conflict.

Scaling `message-api` works too, but note ADR-003's reply-partition caveat: each instance pins
itself to one partition of `messages.read.reply.v1` on startup, derived from its container
hostname, so Read requests on a scaled-out `message-api` still get routed back to the right
instance.

## Seeing the DLT work

Publish something MS-2 can't process. The easiest reliable trigger is a malformed payload sent
directly to a topic, which the consumer's `ErrorHandlingDeserializer` turns into a
non-retryable failure:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic messages.create.v1 \
  --property "parse.key=true" --property "key.separator=:" <<'EOF'
999:not-json
EOF
```

Then check `messages.dlt.v1` in Kafka UI (or
`docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server
localhost:9092 --topic messages.dlt.v1 --from-beginning`) — the record lands there immediately,
with the original headers plus exception metadata, no retries (ADR §5.6: retrying a poison
message forever would block the partition).

A transient failure (e.g. stop `postgres` mid-flight) instead gets exponential backoff up to
the configured retry cap before landing on the DLT — see `message-processor`'s
`RetryProperties` in `application.yml`.

## What I'd add next, and why I didn't now

Per the brief's instruction to keep this simple, the following were deliberately left out:

- **Authentication / API gateway / service discovery** — out of scope for a two-service demo;
  add at the point an actual second consumer of the API exists.
- **Schema registry (Avro/Protobuf)** — the shared `contracts` module does this job at
  acceptable cost for two services under one team; see ADR-002 for where that stops scaling.
- **Kubernetes manifests** — Compose is sufficient to demonstrate scale-out; k8s adds ordinals,
  readiness gates and a scheduler with nothing here that needs them yet.
- **Distributed tracing infrastructure** — correlation IDs propagated through MDC and Kafka
  headers give end-to-end traceability today; a collector (Zipkin/Tempo) is the natural next
  step once there's a third hop to trace across.
- **Transactional outbox / saga** — MS-1 has no database, so there's no dual-write to protect
  against (ADR §5.8); a saga has no multi-step transaction to coordinate here.
- **Caching, rate limiting** — no read-heavy hot path or noisy-neighbor problem in this demo to
  justify either.
- **A `processed_events` dedup table** — natural idempotency (upsert/delete-if-exists) covers
  Kafka's at-least-once delivery here at far lower write cost; see ADR-006.

## Project layout

```
contracts/            shared event schemas and topic/header constants — nothing else
message-api/           MS-1: REST in, Kafka out
message-processor/      MS-2: Kafka in, DB out
docker/                docker-compose.yml (Dockerfiles live next to each service's pom.xml)
docs/adr/              one file per architectural decision
```
