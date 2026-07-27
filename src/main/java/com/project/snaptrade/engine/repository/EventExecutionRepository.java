package com.project.snaptrade.engine.repository;

import com.project.snaptrade.engine.domain.OrderEvent;
import com.project.snaptrade.engine.domain.OrderProjectionSnapshot;
import com.project.snaptrade.engine.domain.Trade;
import com.project.snaptrade.engine.domain.constant.EventType;
import com.project.snaptrade.engine.domain.constant.OrderStatus;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

@Slf4j
@Repository
@RequiredArgsConstructor
public class EventExecutionRepository {

    private final JdbcTemplate jdbcTemplate;

    @Transactional
    public void appendEvents(List<Trade> trades, List<OrderEvent> events) {
        if (!trades.isEmpty()) {
            jdbcTemplate.batchUpdate(
                    "INSERT INTO trades (id, market_id, maker_order_id, taker_order_id, buyer_id, seller_id, price, quantity, quote_quantity, maker_fee, taker_fee, sequence_no, traded_at) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())",
                    new BatchPreparedStatementSetter() {
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            Trade t = trades.get(i);
                            ps.setLong(1, t.getId());
                            ps.setLong(2, t.getMarketId());
                            ps.setLong(3, t.getMakerOrderId());
                            ps.setLong(4, t.getTakerOrderId());
                            ps.setLong(5, t.getBuyerId());
                            ps.setLong(6, t.getSellerId());
                            ps.setLong(7, t.getPrice());
                            ps.setLong(8, t.getQuantity());
                            ps.setLong(9, t.getQuoteQuantity());
                            ps.setLong(10, t.getMakerFee());
                            ps.setLong(11, t.getTakerFee());
                            ps.setObject(12, t.getSequenceNo());
                        }
                        public int getBatchSize() { return trades.size(); }
                    });
        }

        if (!events.isEmpty()) {
            jdbcTemplate.batchUpdate(
                    "INSERT INTO order_events (id, order_id, trade_id, event_type, status_before, status_after, fill_qty, fill_price, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    new BatchPreparedStatementSetter() {
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            OrderEvent e = events.get(i);
                            ps.setLong(1, e.getId());
                            ps.setLong(2, e.getOrderId());
                            ps.setObject(3, e.getTradeId());
                            ps.setString(4, e.getEventType().name());
                            ps.setObject(5, e.getStatusBefore() != null ? e.getStatusBefore().name() : null);
                            ps.setObject(6, e.getStatusAfter() != null ? e.getStatusAfter().name() : null);
                            ps.setLong(7, e.getFillQty());
                            ps.setLong(8, e.getFillPrice());
                            ps.setString(9, e.getPayload());
                        }
                        public int getBatchSize() { return events.size(); }
                    });
        }
    }

    @Transactional
    public void updateReadModels(List<OrderProjectionSnapshot> orders) {
        if (!orders.isEmpty()) {
            jdbcTemplate.batchUpdate(
                    "INSERT INTO orders (id, user_id, market_id, side, order_type, time_in_force, price, orig_qty, executed_qty, cumulative_quote_qty, status, sequence_no, created_at, updated_at) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
                            "ON DUPLICATE KEY UPDATE " +
                            "executed_qty = VALUES(executed_qty), " +
                            "cumulative_quote_qty = VALUES(cumulative_quote_qty), " +
                            "status = VALUES(status), " +
                            "updated_at = NOW()",
                    new BatchPreparedStatementSetter() {
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            OrderProjectionSnapshot o = orders.get(i);
                            ps.setLong(1, o.id());
                            ps.setLong(2, o.userId());
                            ps.setLong(3, o.marketId());
                            ps.setString(4, o.side().name());
                            ps.setString(5, o.orderType().name());
                            ps.setString(6, o.timeInForce().name());
                            ps.setLong(7, o.price());
                            ps.setLong(8, o.origQty());
                            ps.setLong(9, o.executedQty());
                            ps.setLong(10, o.cumulativeQuoteQty());
                            ps.setString(11, o.status().name());
                            ps.setObject(12, o.sequenceNo());
                        }
                        public int getBatchSize() { return orders.size(); }
                    });
        }
    }

    public List<OrderEvent> findAllEventsOrderByIdAsc() {
        String sql = "SELECT id, order_id, trade_id, event_type, status_before, status_after, fill_qty, fill_price, payload " +
                "FROM order_events ORDER BY id ASC";

        return jdbcTemplate.query(sql, (rs, rowNum) -> OrderEvent.builder()
                .id(rs.getLong("id"))
                .orderId(rs.getLong("order_id"))
                .tradeId(rs.getObject("trade_id") != null ? rs.getLong("trade_id") : null)
                .eventType(EventType.valueOf(rs.getString("event_type")))
                .statusBefore(rs.getString("status_before") != null ? OrderStatus.valueOf(rs.getString("status_before")) : null)
                .statusAfter(rs.getString("status_after") != null ? OrderStatus.valueOf(rs.getString("status_after")) : null)
                .fillQty(rs.getLong("fill_qty"))
                .fillPrice(rs.getLong("fill_price"))
                .payload(rs.getString("payload"))
                .build());
    }
}