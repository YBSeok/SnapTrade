package com.project.snaptrade.market.domain.constant;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public enum ChartInterval {
    SEC_1("1s"),
    MIN_1("1m"),
    MIN_5("5m"),
    MIN_10("10m"),
    MIN_15("15m"),
    MIN_30("30m"),
    HOUR_1("1h"),
    HOUR_4("4h"),
    DAY_1("1d"),
    WEEK_1("1w"),
    MONTH_1("1M"),
    YEAR_1("1y");

    private final String code;
    private static final ZoneId UTC_ZONE = ZoneId.of("UTC");

    ChartInterval(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public long getOpenTimeMs(long currentMs) {
        ZonedDateTime now = Instant.ofEpochMilli(currentMs).atZone(UTC_ZONE);
        ZonedDateTime openTime;

        switch (this) {
            case SEC_1 -> openTime = now.truncatedTo(ChronoUnit.SECONDS);
            case MIN_1 -> openTime = now.truncatedTo(ChronoUnit.MINUTES);
            case MIN_5 -> openTime = now.truncatedTo(ChronoUnit.MINUTES).withMinute((now.getMinute() / 5) * 5);
            case MIN_10 -> openTime = now.truncatedTo(ChronoUnit.MINUTES).withMinute((now.getMinute() / 10) * 10);
            case MIN_15 -> openTime = now.truncatedTo(ChronoUnit.MINUTES).withMinute((now.getMinute() / 15) * 15);
            case MIN_30 -> openTime = now.truncatedTo(ChronoUnit.MINUTES).withMinute((now.getMinute() / 30) * 30);
            case HOUR_1 -> openTime = now.truncatedTo(ChronoUnit.HOURS);
            case HOUR_4 -> openTime = now.truncatedTo(ChronoUnit.HOURS).withHour((now.getHour() / 4) * 4);
            case DAY_1 -> openTime = now.truncatedTo(ChronoUnit.DAYS);
            case WEEK_1 -> {
                // 월요일을 주의 시작으로 간주 (ISO 기준)
                int daysToSubtract = now.getDayOfWeek().getValue() - 1;
                openTime = now.truncatedTo(ChronoUnit.DAYS).minusDays(daysToSubtract);
            }
            case MONTH_1 -> openTime = now.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1);
            case YEAR_1 -> openTime = now.truncatedTo(ChronoUnit.DAYS).withDayOfYear(1);
            default -> throw new IllegalArgumentException("Unsupported interval");
        }
        return openTime.toInstant().toEpochMilli();
    }
}