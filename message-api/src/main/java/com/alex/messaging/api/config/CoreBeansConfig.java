package com.alex.messaging.api.config;

import com.alex.messaging.api.application.EventIdGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.UUID;

/**
 * Small framework-level collaborators (clock, id generator) as beans rather than static calls,
 * so anything depending on them can be unit-tested without mocking statics (CLAUDE.md §5.11).
 */
@Configuration
public class CoreBeansConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    EventIdGenerator eventIdGenerator() {
        return UUID::randomUUID;
    }
}
