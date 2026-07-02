package com.vaibhav.ratelimiter.openbanking.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

/**
 * Each algorithm's check-and-update happens in a single Lua script executed atomically by
 * Redis — no read-modify-write race between checking the limit and consuming from it, even
 * under concurrent requests from the same partner.
 */
@Configuration
public class RateLimiterScriptConfig {

    @Bean
    public RedisScript<List> fixedWindowScript() {
        return script("scripts/fixed-window.lua");
    }

    @Bean
    public RedisScript<List> slidingWindowLogScript() {
        return script("scripts/sliding-window-log.lua");
    }

    @Bean
    public RedisScript<List> slidingWindowCounterScript() {
        return script("scripts/sliding-window-counter.lua");
    }

    @Bean
    public RedisScript<List> tokenBucketScript() {
        return script("scripts/token-bucket.lua");
    }

    @Bean
    public RedisScript<List> leakyBucketScript() {
        return script("scripts/leaky-bucket.lua");
    }

    private RedisScript<List> script(String classpathLocation) {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(classpathLocation));
        script.setResultType(List.class);
        return script;
    }
}
