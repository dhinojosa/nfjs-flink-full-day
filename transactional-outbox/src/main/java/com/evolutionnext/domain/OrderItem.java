package com.evolutionnext.domain;

import java.math.BigDecimal;

public class OrderItem {
    private final String product;
    private final int quantity;
    private final BigDecimal price;

    public OrderItem(String product, int quantity, BigDecimal price) {
        this.product = product;
        this.quantity = quantity;
        this.price = price;
    }

    public BigDecimal getSubtotal() {
        return price.multiply(BigDecimal.valueOf(quantity));
    }

    public String getProduct() {
        return product;
    }

    public int getQuantity() {
        return quantity;
    }

    public BigDecimal getPrice() {
        return price;
    }
}
