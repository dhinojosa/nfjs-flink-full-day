package com.evolutionnext.application;

import com.evolutionnext.domain.Order;
import com.evolutionnext.port.out.OrderRepository;

public class OrderService {
    private final OrderRepository repository;

    public OrderService(OrderRepository repository) {
        this.repository = repository;
    }

    public void placeOrder(Order order) {
        repository.save(order);
    }
}
