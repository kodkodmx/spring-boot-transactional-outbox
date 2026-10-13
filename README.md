# Spring Boot Transactional Outbox (minimal)

Saving a row and publishing a Kafka event in the same request looks simple until one of them fails. This repo is a small, runnable example of the transactional outbox pattern with Spring Boot 4.1, JPA and Kafka: the event is written in the same database transaction as the business data, and a relay publishes it afterwards. The tests run with embedded Kafka and H2, so `./mvnw verify` works without Docker. It's one focused piece of the Spring Boot 4 Kafka Event Starter (see "Full kits" below).

## What problem it solves

A service often has to save a row in its database **and** publish an event to Kafka.
Doing both directly is unsafe: the DB commit can succeed and the Kafka send fail (lost event),
or the send succeed and the DB roll back (event for data that does not exist).

The outbox pattern fixes this:

1. The business row and an `outbox_events` row are written in **one database transaction**.
2. A separate relay polls pending outbox rows, sends them to Kafka and marks them published.

The result is at-least-once delivery (a crash between the Kafka send and the commit can still resend, which is why the header exists) that never publishes uncommitted data. Each message carries
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
| `retryAfterFailedSendDeliversExactlyOnce` | First send fails, row stays pending; the retry delivers it and exactly one record (same `eventId`) reaches Kafka; further runs send nothing. |
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

This example is free under MIT and stays small on purpose. If you want the parts listed in "Limits of this example" already wired together, the same author sells two source-code kits on Payhip:

- **Spring Boot 4 Kafka Event Starter** (25 USD): transactional outbox, idempotent consumers, exponential retries with a dead-letter topic, a sample Order to Inventory saga, Docker Compose (PostgreSQL + Kafka KRaft) and CI.
  https://payhip.com/b/mH2oM
- **Spring Boot 4 API Starter** (29 USD): REST API with JWT and rotating refresh tokens, PostgreSQL/Flyway, Testcontainers tests and Docker.
  https://payhip.com/b/wXQ7a

You don't need either kit to use the pattern here.

## License

MIT, see [LICENSE](LICENSE).
