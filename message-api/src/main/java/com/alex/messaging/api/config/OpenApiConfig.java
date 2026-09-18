package com.alex.messaging.api.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    OpenAPI messageApiOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Message API (MS-1)")
                .description("Publishes Create/Update/Delete/Read requests to Kafka for MS-2 to process.")
                .version("v1"));
    }
}
