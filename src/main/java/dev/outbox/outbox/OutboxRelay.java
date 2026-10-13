package dev.outbox.outbox;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Polls unpublished outbox rows, sends them to Kafka and marks them published.
 * Delivery is at-least-once: the event id travels in the "eventId" header so
 * consumers can de-duplicate. On a failed send the batch stops and the row
 * stays pending for the next run, which keeps per-key ordering.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, String> kafka;
    private final int batchSize;

    public OutboxRelay(OutboxEventRepository repository, KafkaTemplate<String, String> kafka,
                       @Value("${app.outbox.batch-size:50}") int batchSize) {
        this.repository = repository;
        this.kafka = kafka;
        this.batchSize = batchSize;
    }

    @Scheduled(initialDelayString = "${app.outbox.initial-delay-ms:1000}",
               fixedDelayString = "${app.outbox.poll-interval-ms:500}")
    @Transactional
    public int publishPending() {
        List<OutboxEvent> batch = repository.findByPublishedAtIsNullOrderByCreatedAtAsc(Limit.of(batchSize));
        int sent = 0;
        for (OutboxEvent event : batch) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(event.getTopic(), event.getMessageKey(), event.getPayload());
            record.headers().add("eventId", event.getId().toString().getBytes(StandardCharsets.UTF_8));
            try {
                kafka.send(record).get(10, TimeUnit.SECONDS);
            } catch (Exception ex) {
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                log.warn("Outbox send failed for eventId={}, will retry: {}", event.getId(), ex.toString());
                break;
            }
            event.markPublished(Instant.now());
            sent++;
        }
        return sent;
    }
}
