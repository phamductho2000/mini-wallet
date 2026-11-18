package com.dudu.miniwallet.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransferEventListener {
    private static final Logger log = LoggerFactory.getLogger(TransferEventListener.class);
    private final JdbcTemplate jdbc;

    public TransferEventListener(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @KafkaListener(topics = "transfer.completed")
    @Transactional
    public void onTransferCompleted(String payload,
                                    @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                    @Header(KafkaHeaders.OFFSET) long offset) {
        // Idempotent consumer: key nghiệp vụ = transferId (nằm trong payload). Dùng partition-offset là cách thay thế.
        String eventKey = "transfer:" + extract(payload, "transferId");

        int inserted = jdbc.update(
                "INSERT INTO processed_event(event_key) VALUES (?) ON CONFLICT DO NOTHING", eventKey);
        if (inserted == 0) {
            log.info("DUPLICATE event {} (p{}-o{}), skip", eventKey, partition, offset);
            return;
        }

        // Thử demo DLT: gửi transfer có "from":"POISON" để ném lỗi -> retry 3 lần -> vào transfer.completed.DLT
        if (payload.contains("POISON")) throw new IllegalStateException("poison message");

        log.info("📨 Notify user: {} (p{}-o{})", payload, partition, offset);
    }

    private String extract(String json, String field) {
        int i = json.indexOf("\"" + field + "\":");
        int s = i + field.length() + 3;
        int e = s;
        while (e < json.length() && (Character.isDigit(json.charAt(e)))) e++;
        return json.substring(s, e);
    }
}
