# Spring Boot Transactional Outbox (minimal)

<!-- MARKY: copy pending -->
A small, runnable example of the transactional outbox pattern with Spring Boot 4.1, JPA and Kafka.
It is one focused piece extracted from the Spring Boot 4 Kafka Event Starter (see "Full kits" below).

## What problem it solves

A service often has to save a row in its database **and** publish an event to Kafka.
Doing both directly is unsafe: the DB commit can succeed and the Kafka send fail (lost event),
or the send succeed and the DB roll back (event for data that does not exist).

The outbox pattern fixes this:

1. The business row and an `outbox_events` row are written in **one database transaction**.
2. A separate relay polls pending outbox rows, sends them to Kafka and marks them published.

The result is at-least-once delivery that never publishes uncommitted data. Each message carries
an `eventId` header so consumers can de-duplicate.

## Requirements

- Java 21
- No Docker needed for the tests (embedded Kafka + in-memory H2)
- To run the app itself you need a Kafka broker on `localhost:9092` (or set `KAFKA_BOOTSTRAP_SERVERS`)

## Run

```bash
./mvnw -B clean verify      # build + all tests
./mvnw spring-boot:run      # run against a local Kafka
```

## How the tests prove it

`OutboxIntegrationTest` (Spring context, embedded Kafka, H2):

| Test | What it shows |
|---|---|
| `orderAndOutboxEventAreCommittedTogether` | The order and its outbox event are saved together; the event starts unpublished. |
| `rollbackLeavesNeitherOrderNorEvent` | If the surrounding transaction fails, neither row exists. |
| `validationFailureWritesNothing` | A rejected request leaves no order and no event. |
| `outboxWriterRefusesToRunWithoutTransaction` | `OutboxWriter` uses `Propagation.MANDATORY`, so it cannot be called outside a transaction by mistake. |
| `relayPublishesMarksSentAndDoesNotResend` | The relay sends the record (key = order id, `eventId` header), marks it published, and a second run sends nothing. |

`OutboxRelayRetryTest` (plain unit test, mocked `KafkaTemplate`):

| Test | What it shows |
|---|---|
| `failedSendKeepsEventPendingAndStopsBatchThenRetrySucceeds` | On a broker error the row stays pending and the batch stops (keeps order); the next run publishes it. |

## Project layout

```
src/main/java/dev/outbox/
  OutboxDemoApplication.java      Spring Boot app, scheduling enabled
  order/
    PurchaseOrder.java            example business entity
    PurchaseOrderRepository.java
    OrderService.java             writes order + outbox event in one @Transactional method
  outbox/
    OutboxEvent.java              outbox row (topic, key, payload, createdAt, publishedAt)
    OutboxEventRepository.java
    OutboxWriter.java             appends an event; requires an existing transaction
    OutboxRelay.java              @Scheduled poller: send to Kafka, then mark published
src/main/resources/application.yml
src/test/java/dev/outbox/
  OutboxIntegrationTest.java
  OutboxRelayRetryTest.java
```

Settings (`application.yml`): `app.topics.orders`, `app.outbox.batch-size`,
`app.outbox.poll-interval-ms`, `app.outbox.initial-delay-ms`.

## Limits of this example

Kept small on purpose. It does not include: idempotent consumers, retries with dead-letter topics,
Flyway migrations, PostgreSQL, multi-instance relay locking, outbox cleanup, Docker Compose or CI.

## Full kits

This repo is free. The complete, paid kits by the same author are on Payhip:

- **Spring Boot 4 Kafka Event Starter**: transactional outbox, idempotent consumers, retries + dead-letter topics, sample saga.
  https://payhip.com/b/mH2oM
- **Spring Boot 4 API Starter**: REST API with JWT + rotating refresh tokens, roles, PostgreSQL/Flyway.
  https://payhip.com/b/wXQ7a

## License

MIT, see [LICENSE](LICENSE).
