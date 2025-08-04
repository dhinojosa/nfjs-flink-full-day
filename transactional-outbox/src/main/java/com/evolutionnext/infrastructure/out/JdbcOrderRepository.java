package com.evolutionnext.infrastructure.out;


import com.evolutionnext.domain.Order;
import com.evolutionnext.domain.OrderItem;
import com.evolutionnext.port.out.OrderRepository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JdbcOrderRepository implements OrderRepository {
    private final DataSource dataSource;

    public JdbcOrderRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Order order) {
        String insertOrder = "INSERT INTO orders(id, total) VALUES (?, ?)";
        String insertItem = "INSERT INTO order_items(order_id, product, quantity, price) VALUES (?, ?, ?, ?)";
        String insertOutbox = "INSERT INTO outbox(order_id, total) VALUES (?, ?)";

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (
                PreparedStatement stmtOrder = conn.prepareStatement(insertOrder);
                PreparedStatement stmtItem = conn.prepareStatement(insertItem);
                PreparedStatement stmtOutbox = conn.prepareStatement(insertOutbox)
            ) {
                stmtOrder.setObject(1, order.getId());
                stmtOrder.setBigDecimal(2, order.getTotal());
                stmtOrder.executeUpdate();

                for (OrderItem item : order.getItems()) {
                    stmtItem.setObject(1, order.getId());
                    stmtItem.setString(2, item.getProduct());
                    stmtItem.setInt(3, item.getQuantity());
                    stmtItem.setBigDecimal(4, item.getPrice());
                    stmtItem.executeUpdate();
                }

                stmtOutbox.setObject(1, order.getId());
                stmtOutbox.setBigDecimal(2, order.getTotal());
                stmtOutbox.executeUpdate();

                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw new RuntimeException("Transaction failed", e);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
