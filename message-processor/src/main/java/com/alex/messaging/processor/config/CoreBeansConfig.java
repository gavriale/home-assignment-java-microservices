package com.alex.messaging.processor.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
public class CoreBeansConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
