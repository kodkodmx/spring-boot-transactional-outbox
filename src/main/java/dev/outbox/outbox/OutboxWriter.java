package dev.outbox.outbox;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/** Appends an event to the outbox. Must run inside the caller's transaction. */
@Component
public class OutboxWriter {

    private final OutboxEventRepository repository;

    public OutboxWriter(OutboxEventRepository repository) {
        this.repository = repository;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public UUID enqueue(String topic, String key, String payload) {
        UUID id = UUID.randomUUID();
        repository.save(new OutboxEvent(id, topic, key, payload, Instant.now()));
        return id;
    }
}
