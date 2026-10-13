package dev.outbox;

import dev.outbox.outbox.OutboxEvent;
import dev.outbox.outbox.OutboxEventRepository;
import dev.outbox.outbox.OutboxRelay;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Broker failures: the row stays pending, later rows wait, and the next run retries. */
class OutboxRelayRetryTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final OutboxEventRepository repository = mock(OutboxEventRepository.class);
    private final OutboxRelay relay = new OutboxRelay(repository, kafka, 50);

    private static OutboxEvent event(String key) {
        return new OutboxEvent(UUID.randomUUID(), "order.events", key, "{}", Instant.now());
    }

    @Test
    @SuppressWarnings("unchecked")
    void failedSendKeepsEventPendingAndStopsBatchThenRetrySucceeds() {
        OutboxEvent first = event("a");
        OutboxEvent second = event("b");
        when(repository.findByPublishedAtIsNullOrderByCreatedAtAsc(any(Limit.class)))
                .thenReturn(List.of(first, second));
        when(kafka.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        assertThat(relay.publishPending()).isZero();
        assertThat(first.isPublished()).isFalse();
        assertThat(second.isPublished()).isFalse();
        verify(kafka, times(1)).send(any(ProducerRecord.class));

        assertThat(relay.publishPending()).isEqualTo(2);
        assertThat(first.isPublished()).isTrue();
        assertThat(second.isPublished()).isTrue();
        verify(kafka, times(3)).send(any(ProducerRecord.class));
    }
}
