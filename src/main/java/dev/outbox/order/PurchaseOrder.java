package dev.outbox.order;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.UUID;

@Entity
@Table(name = "purchase_orders")
public class PurchaseOrder {

    @Id
    private UUID id;

    @Column(nullable = false, length = 120)
    private String product;

    @Column(nullable = false)
    private int quantity;

    protected PurchaseOrder() {}

    public PurchaseOrder(UUID id, String product, int quantity) {
        this.id = id;
        this.product = product;
        this.quantity = quantity;
    }

    public UUID getId() { return id; }
    public String getProduct() { return product; }
    public int getQuantity() { return quantity; }
}
