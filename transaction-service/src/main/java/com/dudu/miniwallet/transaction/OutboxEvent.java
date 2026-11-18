package com.dudu.miniwallet.transaction;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "outbox_event")
public class OutboxEvent {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String aggregateId;   // dùng làm Kafka key => cùng ví cùng partition => giữ thứ tự
    private String topic;
    @Column(columnDefinition = "text")
    private String payload;
    private boolean sent = false;
    private Instant createdAt = Instant.now();

    protected OutboxEvent() {}
    public OutboxEvent(String aggregateId, String topic, String payload) {
        this.aggregateId = aggregateId; this.topic = topic; this.payload = payload;
    }
    public Long getId() { return id; }
    public String getAggregateId() { return aggregateId; }
    public String getTopic() { return topic; }
    public String getPayload() { return payload; }
    public void markSent() { this.sent = true; }
}
