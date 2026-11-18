package com.dudu.miniwallet.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.util.concurrent.TimeUnit;

/**
 * Transactional Outbox relay: quét outbox_event chưa gửi -> publish Kafka -> đánh dấu sent.
 * Gửi xong mà crash trước khi markSent => gửi lại => đây là at-least-once, nên consumer PHẢI idempotent.
 * (Bản nâng cao: thay relay này bằng Debezium CDC đọc bảng outbox_event.)
 */
@Component
public class OutboxRelay {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private final OutboxRepository repo;
    private final KafkaTemplate<String, String> kafka;

    public OutboxRelay(OutboxRepository repo, KafkaTemplate<String, String> kafka) {
        this.repo = repo; this.kafka = kafka;
    }

    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void relay() {
        for (OutboxEvent e : repo.findTop100BySentFalseOrderByIdAsc()) {
            try {
                // key = walletId => cùng ví vào cùng partition => đúng thứ tự
                kafka.send(e.getTopic(), e.getAggregateId(), e.getPayload()).get(5, TimeUnit.SECONDS);
                e.markSent();
                repo.save(e);
            } catch (Exception ex) {
                log.warn("Relay failed for outbox {}, will retry: {}", e.getId(), ex.toString());
                break; // giữ thứ tự: lỗi thì dừng, vòng sau thử lại
            }
        }
    }
}
