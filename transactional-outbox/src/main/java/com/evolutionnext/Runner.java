package com.evolutionnext;

import com.evolutionnext.application.OrderService;

import java.math.RoundingMode;
import java.util.Random;

import com.github.javafaker.Faker;
import com.evolutionnext.domain.Order;
import com.evolutionnext.infrastructure.out.DataSourceProvider;
import com.evolutionnext.infrastructure.out.JdbcOrderRepository;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicBoolean;

public class Runner {
    public static void main(String[] args) throws InterruptedException {
        var ds = DataSourceProvider.create();
        var repo = new JdbcOrderRepository(ds);
        var service = new OrderService(repo);
        var faker = new Faker();
        var random = new Random();
        AtomicBoolean done = new AtomicBoolean(false);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            done.set(true);
        }));

        while (!done.get()) {
            var order = new Order();
            int orderItems = random.nextInt(4) + 1;
            for (int i = 0; i < orderItems; i++) {
                int randomQuantity = random.nextInt(10) + 1;
                BigDecimal randomPrice = BigDecimal.valueOf(random.nextDouble() * 100.0).setScale(2, RoundingMode.HALF_UP);
                order.addItem(faker.commerce().productName(), randomQuantity, randomPrice);
            }
            service.placeOrder(order);
            System.out.println("Order placed: " + order.getId());
            Thread.sleep(random.nextInt(30000));
        }
    }
}
