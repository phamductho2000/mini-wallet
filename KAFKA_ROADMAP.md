# Lộ trình học Kafka: 1 broker → cluster

Mỗi chặng: **Làm → Quan sát → Câu phỏng vấn**. Tick `[x]` khi xong. Ghi 3 dòng "thấy gì / vì sao / đánh đổi" ở cuối mỗi chặng.

Compose: `docker-compose.yml` (1 broker, chặng 0-4) · `docker-compose.3broker.yml` (chặng 5-6).

## Phần 1 — 1 broker

### Chặng 0. Nền tảng
- [ ] `docker exec -it <kafka> bash`, tạo topic 3 partition bằng `kafka-topics.sh`
- [ ] Gửi tin có key bằng `kafka-console-producer.sh` (`--property parse.key=true`), đọc lại bằng console-consumer
- [ ] `kafka-consumer-groups.sh --describe`: xem offset, lag; reset offset để replay
- Quan sát: cùng key → cùng partition.
- Hỏi: partition/offset/consumer group là gì? Vì sao ordering chỉ trong 1 partition?

### Chặng 1. Producer đáng tin
- [ ] Đổi `acks` = 0 / 1 / all
- [ ] Bật/tắt `enable.idempotence`; ép lỗi mạng xem tin trùng
- [ ] Thử `linger.ms`, `batch.size`, `compression.type`
- Hỏi: `acks` khác nhau thế nào? Idempotent producer chống trùng bằng gì (producer id + sequence)?

### Chặng 2. Consumer sâu
- [ ] Chạy 2, 3 instance notification-service (đổi `server.port`)
- [ ] Kill 1 instance → xem rebalance; thử 4 consumer trên 3 partition
- [ ] Xử lý lâu hơn `max.poll.interval.ms`
- [ ] Làm bài DLT (README bài 6), thử thêm retry topic
- Hỏi: rebalance là gì, gây hại gì? Commit offset trước hay sau xử lý?

### Chặng 3. Delivery semantics & Exactly-once
- [ ] Ôn README bài 5 (outbox + consumer idempotent)
- [ ] Kafka transactions: `transactional.id` ở producer, `isolation.level=read_committed` ở consumer
- Hỏi: at-most/at-least/exactly-once? EOS của Kafka chỉ đúng trong phạm vi Kafka; ghi DB ngoài vẫn cần idempotency.

### Chặng 4. Retention & compaction
- [ ] Topic `wallet-balance` với `cleanup.policy=compact`, gửi nhiều tin cùng key
- [ ] Chỉnh `retention.ms`, `segment.bytes` nhỏ để thấy segment bị xoá
- [ ] Tăng số partition của topic → key đổi partition
- Hỏi: retention theo thời gian vs dung lượng? Compaction dùng khi nào? Vì sao không giảm được partition?

## Phần 2 — Cluster

### Chặng 5. 3 broker, replication
- [ ] `docker compose down -v` (xoá volume cũ!), bật cluster: xem header `docker-compose.3broker.yml`
- [ ] Chạy app với `SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9094,localhost:9096` và `APP_KAFKA_REPLICAS=3`
- [ ] `kafka-topics.sh --describe`: ghi lại Leader / Replicas / Isr
- [ ] `docker stop kafka-2` → còn ghi được? Leader đổi?
- [ ] `docker stop kafka-3` → ghi lỗi `NotEnoughReplicas`; outbox giữ event, bật lại thì relay gửi bù
- Hỏi: leader/follower, ISR, high watermark? `min.insync.replicas` + `acks=all` bảo vệ gì? Unclean leader election đánh đổi gì?

### Chặng 6. Thêm broker, chia lại partition
- [ ] `docker compose -f docker-compose.3broker.yml --profile scale up -d kafka-4`
- [ ] Broker 4 trống? Tạo topic mới có dùng broker 4 không?
- [ ] `kafka-reassign-partitions.sh` (`--generate` → `--execute` → `--verify`)
- Hỏi: thêm broker có tự rebalance không? Tăng RF topic cũ thế nào? Preferred leader election?

### Chặng 7. Vận hành & theo dõi
- [ ] Prometheus + Grafana (JMX exporter) cho Kafka
- [ ] Theo dõi: consumer lag, under-replicated partitions, request latency
- [ ] Tải thử bằng `kafka-producer-perf-test.sh`
- Hỏi: theo dõi Kafka bằng chỉ số nào? Lag tăng thì debug từ đâu?

### Chặng 8. Hệ sinh thái (khớp CV)
- [ ] Kafka Connect + Debezium đọc bảng `outbox_event` thay `OutboxRelay`
- [ ] Sink connector → OpenSearch
- [ ] (Tuỳ chọn) Schema Registry + Avro
- Hỏi: CDC khác polling? Debezium đọc WAL thế nào? Vì sao chọn Debezium thay outbox polling?

## Ưu tiên nếu sắp phỏng vấn
Chặng 1, 2, 3, 5, 8.
