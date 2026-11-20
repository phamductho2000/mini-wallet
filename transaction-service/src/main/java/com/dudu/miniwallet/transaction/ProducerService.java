package com.dudu.miniwallet.transaction;

import jakarta.annotation.PostConstruct;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class ProducerService {

    private final KafkaTemplate<String, String> kafka;

    public ProducerService(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;
    }

    @PostConstruct
    public void init() {
        testKafka();
    }

    public void testKafka() {
        for (int i = 0; i < 10000; i++) {
            kafka.send("test.completed", "test" + i);
        }
    }
}
