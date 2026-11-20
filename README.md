# mini-wallet — project luyện Java backend

Mini ví điện tử để luyện: **OpenFeign, Kafka, Transactional Outbox, Idempotency, Idempotent Consumer, DLT, concurrency (atomic update)**.
Stack: Java 21, Spring Boot 3.3, Spring Cloud 2023.0, PostgreSQL 16, Kafka (KRaft).

```
client ──POST /transfers (Idempotency-Key)──► transaction-service :8082
                                                │  Feign (timeout, NEVER_RETRY, ErrorDecoder)
                                                ▼
                                         wallet-service :8081   (UPDATE ... WHERE balance >= amount)
transaction-service ─ outbox_event ─(@Scheduled relay)─► Kafka topic transfer.completed (key = walletId)
                                                              ▼
                                         notification-service :8083 (processed_event + DLT)
```

## Chạy
```bash
docker compose up -d                         # postgres, kafka, redis, kafka-ui (http://localhost:8080)
mvn -DskipTests package
java -jar wallet-service/target/*.jar &
java -jar transaction-service/target/*.jar &
java -jar notification-service/target/*.jar &
```

## Thử nhanh
```bash
curl -XPOST localhost:8081/wallets -H 'Content-Type: application/json' -d '{"id":"A","balance":1000}'
curl -XPOST localhost:8081/wallets -H 'Content-Type: application/json' -d '{"id":"B","balance":0}'

curl -XPOST localhost:8082/transfers -H 'Content-Type: application/json' -H 'Idempotency-Key: k-001' \
     -d '{"from":"A","to":"B","amount":100}'
# gọi lại y hệt k-001 => trả kết quả cũ, KHÔNG trừ thêm tiền
curl localhost:8081/wallets/A     # 900
```
Xem log notification-service: `📨 Notify user ...`

## Học Kafka từ 1 broker lên cluster
Xem [docs/KAFKA_ROADMAP.md](docs/KAFKA_ROADMAP.md). Cluster 3 broker: `docker-compose.3broker.yml`.

## Luyện scale: từ tải thấp lên cao
Xem [docs/SCALING_LAB.md](docs/SCALING_LAB.md) (k6 `scripts/load.js`, seed `scripts/seed-wallets.sh`, monitoring `docker-compose.monitoring.yml`). Giải thích config: [docs/KAFKA_CONFIG.md](docs/KAFKA_CONFIG.md).

## Bài tập (mỗi bài ứng với 1 câu phỏng vấn)
1. **Idempotency**: bắn 20 request song song cùng `Idempotency-Key` (`hey`/`k6`/`xargs -P`) → chỉ trừ 1 lần. Giải thích vai trò unique index.
2. **Race condition**: tạo ví A=100, bắn 50 request debit 10 song song với các key khác nhau → số dư không âm, đúng 10 request thành công. Sau đó đổi `debit` sang `findById` + set + save (read-modify-write) để thấy lost update; rồi thử `@Version` và Redisson lock, so sánh.
3. **Feign timeout**: thêm `Thread.sleep(5000)` vào `WalletController.debit` → transfer thành `UNKNOWN`. Hỏi: tiền đã trừ chưa? Vì sao KHÔNG retry mù? Viết job đối soát (query lại wallet theo transfer id).
4. **Retry an toàn**: thêm cột `transfer_id`/`request_id` vào debit ở wallet-service + bảng `wallet_tx` unique → lúc đó mới bật retry cho debit được.
5. **Outbox + at-least-once**: tắt app giữa lúc relay (sau `kafka.send`, trước `markSent`) → event gửi 2 lần → notification-service log `DUPLICATE ... skip`.
6. **DLT**: transfer với `"from":"POISON"` (cần ví POISON tồn tại) → consumer ném lỗi, retry 3 lần rồi vào `transfer.completed.DLT` (xem Kafka UI).
7. **Ordering/partition**: topic 3 partition, key = walletId → kiểm tra cùng ví luôn vào cùng partition. Thử scale 2 instance notification-service để thấy rebalance.
8. **Saga/compensation**: khi debit OK mà credit lỗi → hiện đang đánh dấu `UNKNOWN`; viết bước bù (credit lại ví nguồn) + trạng thái `COMPENSATED`.
9. **Test**: viết integration test bằng Testcontainers (Postgres + Kafka) cho case 1, 2, 5.

## Nâng cấp sau (khớp CV)
Debezium CDC đọc `outbox_event` thay relay → OpenSearch · Eureka + Gateway · Resilience4j (circuit breaker, fallback — nhớ chỉnh TimeLimiter) · Redis/Redisson lock · Micrometer + Prometheus/Grafana.
