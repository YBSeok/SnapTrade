package com.project.snaptrade.common.config;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DatabaseInitializer {

    private final JdbcTemplate jdbcTemplate;

    @EventListener(ApplicationReadyEvent.class)
    @Transactional
    public void clearMatchingTables() {
        log.info("[System] 매칭 엔진 관련 테이블(order, orderevent, trade) 초기화를 시작합니다.");

//        jdbcTemplate.execute("SET FOREIGN_KEY_CHECKS = 0;");
//
//        jdbcTemplate.execute("TRUNCATE TABLE trade;");
//        jdbcTemplate.execute("TRUNCATE TABLE orderevent;");
//
//        jdbcTemplate.execute("TRUNCATE TABLE `order`;");
//
//        jdbcTemplate.execute("SET FOREIGN_KEY_CHECKS = 1;");

        log.info("[System] 매칭 엔진 테이블 초기화가 완료되었습니다.");
    }
}
