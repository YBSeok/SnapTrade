package com.project.snaptrade.market.repository;

import com.project.snaptrade.market.domain.Ticker;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class TickerJdbcRepository {

    private final JdbcTemplate jdbcTemplate;

    public void batchUpsert(List<Ticker> tickers) {
        if (tickers == null || tickers.isEmpty()) {
            return;
        }

        String sql = "INSERT INTO tickers (" +
                "market_id, last_price, price_change, price_change_pct, " +
                "high_24h, low_24h, volume_24h, quote_volume_24h, trade_count_24h, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                "ON DUPLICATE KEY UPDATE " +
                "last_price = VALUES(last_price), " +
                "price_change = VALUES(price_change), " +
                "price_change_pct = VALUES(price_change_pct), " +
                "high_24h = VALUES(high_24h), " +
                "low_24h = VALUES(low_24h), " +
                "volume_24h = VALUES(volume_24h), " +
                "quote_volume_24h = VALUES(quote_volume_24h), " +
                "trade_count_24h = VALUES(trade_count_24h), " +
                "updated_at = CURRENT_TIMESTAMP";

        jdbcTemplate.batchUpdate(sql, tickers, tickers.size(), (PreparedStatement ps, Ticker ticker) -> {
            ps.setLong(1, ticker.getMarketId());
            ps.setLong(2, ticker.getLastPrice());
            ps.setLong(3, ticker.getPriceChange());
            ps.setLong(4, ticker.getPriceChangePct());
            ps.setLong(5, ticker.getHigh24h());
            ps.setLong(6, ticker.getLow24h());
            ps.setLong(7, ticker.getVolume24h());
            ps.setLong(8, ticker.getQuoteVolume24h());
            ps.setLong(9, ticker.getTradeCount24h());
        });
    }
}