package com.dudu.miniwallet.transaction;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/** Metric outbox_pending: số event chưa gửi sang Kafka. Tăng mãi = relay không theo kịp tải. */
@Component
public class OutboxMetrics {
    public OutboxMetrics(MeterRegistry registry, OutboxRepository repo) {
        Gauge.builder("outbox.pending", repo, OutboxRepository::countBySentFalse)
                .description("Outbox events not yet published to Kafka")
                .register(registry);
    }
}
