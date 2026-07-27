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

        String sql = "INSERT INTO klines (market_id, `interval`, open_time_ms, open_price, high_price, low_price, close_price, volume, quote_volume) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE " +
                "high_price = VALUES(high_price), " +
                "low_price = VALUES(low_price), " +
                "close_price = VALUES(close_price), " +
                "volume = VALUES(volume), " +
                "quote_volume = VALUES(quote_volume)";

        jdbcTemplate.batchUpdate(sql, klines, klines.size(), (PreparedStatement ps, Kline kline) -> {
            ps.setLong(1, kline.getMarketId());
            ps.setString(2, kline.getInterval());
            ps.setLong(3, kline.getOpenTimeMs());
            ps.setLong(4, kline.getOpenPrice());
            ps.setLong(5, kline.getHighPrice());
            ps.setLong(6, kline.getLowPrice());
            ps.setLong(7, kline.getClosePrice());
            ps.setLong(8, kline.getVolume());
            ps.setLong(9, kline.getQuoteVolume());
        });
    }
}
