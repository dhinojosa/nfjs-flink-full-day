package com.evolutionnext.port.out;

import com.evolutionnext.domain.Order;

public interface OrderRepository {
    void save(Order order);
}
