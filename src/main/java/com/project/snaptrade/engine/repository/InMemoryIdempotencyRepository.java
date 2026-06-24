package com.project.snaptrade.engine.repository;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class InMemoryIdempotencyRepository {
    private final Map<String, Long> cache = new ConcurrentHashMap<>();

    public boolean setNx(String key, long ttlSeconds) {
        long expireAt = System.currentTimeMillis() + (ttlSeconds * 1000);
        Long existing = cache.putIfAbsent(key, expireAt);

        if (existing != null) {
            if (System.currentTimeMillis() > existing) {
                cache.put(key, expireAt);
                return true;
            }
            return false;
        }
        return true;
    }
}
