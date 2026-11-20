# Scaling lab: từ tải thấp, scale dần

Nguyên tắc: **đo → tìm nút thắt → sửa MỘT thứ → đo lại**. Không sửa 2 thứ cùng lúc.

## Chuẩn bị (1 lần)
```bash
docker compose up -d                                   # 1 broker, postgres, kafka-ui
docker compose -f docker-compose.monitoring.yml up -d  # prometheus :9090, grafana :3000
mvn -DskipTests package && chạy 3 service
./scripts/seed-wallets.sh 1000 1000000                 # ví W0..W999, mỗi ví 1.000.000
```
Cài k6 (https://k6.io). Grafana: thêm datasource Prometheus `http://prometheus:9090`.

Chạy tải: `k6 run -e RATE=10 -e DURATION=2m scripts/load.js`

## Chỉ số cần xem (PromQL, vào Prometheus hoặc Grafana)
| Chỉ số | Query |
|---|---|
| Req/s vào transaction-service | `sum(rate(http_server_requests_seconds_count{service="transaction-service",uri="/transfers"}[30s]))` |
| p95 latency | `histogram_quantile(0.95, sum by (le) (rate(http_server_requests_seconds_bucket{service="transaction-service",uri="/transfers"}[30s])))` |
| Outbox chưa gửi | `outbox_pending` |
| Consumer lag | `kafka_consumer_fetch_manager_records_lag_max{service="notification-service"}` |
| Hikari chờ kết nối | `hikaricp_connections_pending` (và `hikaricp_connections_active`) |
| Thread Tomcat bận | `tomcat_threads_busy_threads` |
| CPU/RAM container | `docker stats` |
| Trạng thái transfer | `scripts/queries.sql` |

Xem lag bằng CLI: `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group notification-service`
(nếu có metric nào không hiện, mở `http://localhost:8082/actuator/prometheus` để xem tên chính xác.)

## Bảng ghi chép (copy mỗi bậc một dòng)
| Bậc | RATE | p95 | Lỗi | Lag | outbox_pending | Nút thắt | Đã sửa | Kết quả |
|---|---|---|---|---|---|---|---|---|

---
## Bậc 0 — Baseline, 10 req/s, 1 broker
- Chạy: `RATE=10`. Ghi lại mọi chỉ số làm mốc.
- Kỳ vọng: lag ≈ 0, outbox_pending ≈ 0, p95 thấp, mọi transfer `COMPLETED`.
- [ ] Xong, đã ghi baseline

## Bậc 1 — 100 req/s: nghẽn OutboxRelay (dự đoán)
- Chạy: `RATE=100`, rồi 200, 300 cho đến khi `outbox_pending` tăng không ngừng.
- Vì sao: `OutboxRelay` chạy mỗi 1s, tối đa 100 event/lượt, gửi tuần tự từng cái và chờ `.get(5s)`.
- Sửa (làm lần lượt, đo sau mỗi bước):
  - [ ] Gửi cả lô rồi mới chờ (`send` hết, sau đó `flush()`/chờ các future)
  - [ ] Tăng kích thước lô (`findTop100` → 500/1000) và rút ngắn `fixedDelay`
  - [ ] Producer: `linger.ms=5..20`, `batch.size=32768..131072`, `compression.type=lz4`
- Học được: producer gom batch, đánh đổi độ trễ ↔ thông lượng.

## Bậc 2 — Consumer lag tăng
- Chạy tải đủ cao để `kafka_consumer_fetch_manager_records_lag_max` tăng.
- Sửa lần lượt:
  - [ ] `spring.kafka.listener.concurrency: 3` (bằng số partition)
  - [ ] Chạy thêm instance notification-service (`--server.port=8084`...)
  - [ ] Thử `concurrency: 6` trên 3 partition → 3 luồng ngồi không (vì sao cần tăng partition)
  - [ ] Batch listener + gom ghi DB (1 lần insert nhiều dòng)
- Học được: song song hoá consumer bị chặn bởi số partition.

## Bậc 3 — Đường đồng bộ: Feign + DB + thread
- Dấu hiệu: p95 `POST /transfers` tăng, `hikaricp_connections_pending > 0`, `tomcat_threads_busy` chạm trần.
- Sửa lần lượt:
  - [ ] Chỉnh `spring.datasource.hikari.maximum-pool-size` (thử 10 → 20 → 50, xem Postgres `pg_stat_activity`)
  - [ ] Feign dùng HttpClient/OkHttp có connection pool (thêm dependency `feign-hc5`/`feign-okhttp`)
  - [ ] `server.tomcat.threads.max`
  - [ ] Chạy 2-3 instance transaction-service & wallet-service
- Khi chạy nhiều instance transaction-service, relay gửi TRÙNG event:
  - [ ] Đổi query relay sang `SELECT ... FOR UPDATE SKIP LOCKED` để các instance không tranh cùng event
- Học được: pool to không phải lúc nào cũng tốt; at-least-once vẫn đúng nhờ consumer idempotent.

## Bậc 4 — Mất cân bằng: hot key / hot row
- Chạy: `k6 run -e RATE=100 -e HOT=0.8 scripts/load.js` (80% request dồn vào ví W0).
- Quan sát: 1 partition quá tải (Kafka UI), request vào W0 xếp hàng chờ row lock, p95 tăng dù tổng tải thấp.
- Thử: tăng partition 3 → 6 → 12 (key đổi partition) — hot key vẫn không hết.
- Học được: thêm partition/broker KHÔNG cứu hot key; phải đổi thiết kế (chia nhỏ khoá, gộp thao tác, xử lý bất đồng bộ cho ví nóng).

## Bậc 5 — Khi nào mới cần thêm broker
- Chỉ khi broker là nút thắt thật (CPU/disk/network broker, request latency), hoặc cần chịu lỗi.
- Đo độc lập với app: `kafka-producer-perf-test.sh --topic perf --num-records 500000 --record-size 500 --throughput -1 --producer-props bootstrap.servers=localhost:9092 acks=all`
- Chuyển sang `docker-compose.3broker.yml` (RF=3, min ISR=2), chạy lại đúng lệnh perf-test: **throughput ghi thường GIẢM** vì phải replicate — ghi lại con số.
- [ ] Thêm kafka-4 (`--profile scale`) + `kafka-reassign-partitions.sh`
- Học được: thêm broker để chịu lỗi/dung lượng, không phải phép màu cho throughput.

## Bậc 6 — Bảo vệ khi quá tải
- Đẩy tải gấp 3 mức chịu được.
- [ ] Rate limit ở transaction-service, bulkhead giới hạn request đồng thời tới wallet-service
- [ ] Circuit breaker (Resilience4j) — nhớ chỉnh TimeLimiter ≥ read-timeout của Feign
- Quan sát: hệ thống TỪ CHỐI NHANH (429/503) hay SẬP DÂY CHUYỀN (timeout → UNKNOWN hàng loạt)?

## Bậc 7 — Thay relay bằng Debezium CDC
- [ ] Debezium đọc `outbox_event` (WAL) thay `OutboxRelay`
- So sánh độ trễ event và tải lên DB trước/sau.

## Câu chuyện phỏng vấn (điền số thật của bạn)
"Em đo baseline ở __ req/s thì ổn. Lên __ req/s thì `outbox_pending` tăng vì relay gửi tuần tự, em gom batch và đạt __ req/s. Tiếp đó consumer lag, em tăng concurrency theo số partition... Em chỉ thêm broker khi broker thật sự đầy tài nguyên; ở bậc 5 em đo thấy throughput đổi từ __ xuống __ do replication. Với hot key thì thêm partition không giúp, em phải đổi thiết kế."
