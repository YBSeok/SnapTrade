package com.project.snaptrade.market.repository;

import com.project.snaptrade.market.domain.Kline;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class KlineJdbcRepository {
    private final JdbcTemplate jdbcTemplate;

    public void batchUpsert(List<Kline> klines) {
        if (klines.isEmpty()) return;

        String sql = "INSERT INTO kline (id, market_id, interval_type, open_time_ms, open_price, high_price, low_price, close_price, volume, quote_volume) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE " +
                "high_price = VALUES(high_price), " +
                "low_price = VALUES(low_price), " +
                "close_price = VALUES(close_price), " +
                "volume = VALUES(volume), " +
                "quote_volume = VALUES(quote_volume)";

        jdbcTemplate.batchUpdate(sql, klines, klines.size(), (PreparedStatement ps, Kline kline) -> {
            ps.setLong(1, kline.getId());
            ps.setLong(2, kline.getMarketId());
            ps.setString(3, kline.getInterval());
            ps.setLong(4, kline.getOpenTimeMs());
            ps.setLong(5, kline.getOpen());
            ps.setLong(6, kline.getHigh());
            ps.setLong(7, kline.getLow());
            ps.setLong(8, kline.getClose());
            ps.setLong(9, kline.getVolume());
            ps.setLong(10, kline.getQuoteVolume());
        });
    }
}
