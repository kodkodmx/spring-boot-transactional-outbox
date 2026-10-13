package dev.outbox;

import dev.outbox.order.OrderService;
import dev.outbox.order.PurchaseOrder;
import dev.outbox.order.PurchaseOrderRepository;
import dev.outbox.outbox.OutboxEvent;
import dev.outbox.outbox.OutboxEventRepository;
import dev.outbox.outbox.OutboxRelay;
import dev.outbox.outbox.OutboxWriter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 1, topics = "order.events")
@ActiveProfiles("test")
class OutboxIntegrationTest {

    @Autowired OrderService orderService;
    @Autowired OutboxWriter outboxWriter;
    @Autowired OutboxRelay relay;
    @Autowired PurchaseOrderRepository orders;
    @Autowired OutboxEventRepository outbox;
    @Autowired TransactionTemplate tx;
    @Autowired EmbeddedKafkaBroker broker;

    private Consumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        outbox.deleteAll();
        orders.deleteAll();
        Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "test-" + UUID.randomUUID(), false);
        consumer = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer())
                .createConsumer();
        broker.consumeFromAnEmbeddedTopic(consumer, "order.events");
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void orderAndOutboxEventAreCommittedTogether() {
        PurchaseOrder order = orderService.placeOrder("bike", 2);

        assertThat(orders.findById(order.getId())).isPresent();
        List<OutboxEvent> events = outbox.findAll();
        assertThat(events).hasSize(1);
        assertThat(events.getFirst().getMessageKey()).isEqualTo(order.getId().toString());
        assertThat(events.getFirst().getPayload()).contains("\"orderId\":\"" + order.getId() + "\"");
        assertThat(events.getFirst().isPublished()).isFalse();
    }

    @Test
    void rollbackLeavesNeitherOrderNorEvent() {
        assertThatThrownBy(() -> tx.executeWithoutResult(status -> {
            orderService.placeOrder("bike", 1);
            throw new IllegalStateException("simulated failure after the order was written");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(orders.count()).isZero();
        assertThat(outbox.count()).isZero();
    }

    @Test
    void validationFailureWritesNothing() {
        assertThatThrownBy(() -> orderService.placeOrder("bike", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(orders.count()).isZero();
        assertThat(outbox.count()).isZero();
    }

    @Test
    void outboxWriterRefusesToRunWithoutTransaction() {
        assertThatThrownBy(() -> outboxWriter.enqueue("order.events", "k", "{}"))
                .isInstanceOf(IllegalTransactionStateException.class);
        assertThat(outbox.count()).isZero();
    }

    @Test
    void relayPublishesMarksSentAndDoesNotResend() {
        PurchaseOrder order = orderService.placeOrder("helmet", 3);
        UUID eventId = outbox.findAll().getFirst().getId();

        assertThat(relay.publishPending()).isEqualTo(1);

        ConsumerRecord<String, String> record =
                KafkaTestUtils.getSingleRecord(consumer, "order.events", Duration.ofSeconds(10));
        assertThat(record.key()).isEqualTo(order.getId().toString());
        assertThat(record.value()).contains("OrderPlaced");
        assertThat(new String(record.headers().lastHeader("eventId").value(), StandardCharsets.UTF_8))
                .isEqualTo(eventId.toString());
        assertThat(outbox.findById(eventId).orElseThrow().isPublished()).isTrue();

        // Second run: nothing pending, nothing sent again.
        assertThat(relay.publishPending()).isZero();
        ConsumerRecords<String, String> more = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(2));
        assertThat(more.count()).isZero();
        assertThat(outbox.countByPublishedAtIsNull()).isZero();
    }
}
