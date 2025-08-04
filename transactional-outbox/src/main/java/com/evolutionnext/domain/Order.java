package com.evolutionnext.domain;


import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Order {
    private final UUID id;
    private final List<OrderItem> items = new ArrayList<>();

    public Order() {
        this.id = UUID.randomUUID();
    }

    public void addItem(String product, int quantity, BigDecimal price) {
        items.add(new OrderItem(product, quantity, price));
    }

    public UUID getId() {
        return id;
    }

    public BigDecimal getTotal() {
        return items.stream()
            .map(OrderItem::getSubtotal)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    public List<OrderItem> getItems() {
        return items;
    }
}
