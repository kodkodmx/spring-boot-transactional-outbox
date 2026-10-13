package dev.outbox.order;

import dev.outbox.outbox.OutboxWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Writes the business row and the outbox row in ONE database transaction.
 * Either both are committed or neither is; Kafka is never called here.
 */
@Service
public class OrderService {

    private final PurchaseOrderRepository orders;
    private final OutboxWriter outbox;
    private final String topic;

    public OrderService(PurchaseOrderRepository orders, OutboxWriter outbox,
                        @Value("${app.topics.orders}") String topic) {
        this.orders = orders;
        this.outbox = outbox;
        this.topic = topic;
    }

    @Transactional
    public PurchaseOrder placeOrder(String product, int quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive");
        }
        PurchaseOrder order = orders.save(new PurchaseOrder(UUID.randomUUID(), product, quantity));
        String payload = "{\"type\":\"OrderPlaced\",\"orderId\":\"%s\",\"product\":\"%s\",\"quantity\":%d}"
                .formatted(order.getId(), product.replace("\"", "\\\""), quantity);
        outbox.enqueue(topic, order.getId().toString(), payload);
        return order;
    }
}
