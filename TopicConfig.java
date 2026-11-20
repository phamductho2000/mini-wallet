package com.dudu.miniwallet.transaction;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicConfig {

    // 1 broker: 1 (mặc định). Cluster 3 broker: chạy với APP_KAFKA_REPLICAS=3
    @Value("${app.kafka.replicas:1}")
    private int replicas;

    @Bean
    public NewTopic transferCompleted() {
        return TopicBuilder.name("transfer.completed").partitions(3).replicas(replicas).build();
    }
}
