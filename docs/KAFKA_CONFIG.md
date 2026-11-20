# Giải thích các cấu hình trong mini-wallet

## 1. Broker Kafka (docker-compose)

### Danh tính và vai trò
| Config | Dùng để làm gì |
|---|---|
| `KAFKA_NODE_ID` | ID duy nhất của node trong cluster (1, 2, 3...). Dùng trong `QUORUM_VOTERS` để các node nhận ra nhau. |
| `CLUSTER_ID` | Danh tính của cả cluster, ghi vào `meta.properties` lúc format ổ đĩa. Mọi node phải GIỐNG nhau, khác thì từ chối ghép. Đổi ID mà giữ volume cũ sẽ lỗi → `down -v`. |
| `KAFKA_PROCESS_ROLES` | `broker` = lưu và phục vụ message. `controller` = quản lý metadata (leader, topic...) thay Zookeeper. `broker,controller` = một node làm cả hai (combined mode, hợp dev). Production thường tách riêng. |

### Listener (mạng): phần dễ nhầm nhất
Client kết nối bootstrap → broker trả về **advertised address** → client gọi lại địa chỉ đó. Mỗi nhóm client cần một địa chỉ mà nó với tới được.

| Config | Dùng để làm gì |
|---|---|
| `KAFKA_LISTENERS` | Broker **lắng nghe** ở đâu: `INTERNAL://:29092` (container gọi nhau), `EXTERNAL://:9092` (app trên máy host), `CONTROLLER://:9093` (bầu cử/metadata giữa các node). |
| `KAFKA_ADVERTISED_LISTENERS` | Địa chỉ broker **quảng bá** cho client: `INTERNAL://kafka:29092` (tên service trong mạng Docker), `EXTERNAL://localhost:9092` (từ máy host). Sai chỗ này = lỗi "Connection to node 1 (localhost:9092) could not be established" của kafka-ui. CONTROLLER không cần advertise cho client. |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | Mỗi tên listener dùng giao thức nào. Ở đây đều `PLAINTEXT` (không mã hoá, chỉ để dev). Production dùng SSL/SASL. |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | Các broker nói chuyện với nhau qua listener nào (`INTERNAL`). |
| `KAFKA_CONTROLLER_LISTENER_NAMES` | Listener nào dành cho controller (`CONTROLLER`). |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | Danh sách node tham gia bầu controller dạng `id@host:port`. 1 node: `1@kafka:9093`. 3 node: liệt kê đủ 3. Cần đa số (n/2+1) sống mới có controller. Host phải là tên mà các node gọi được nhau (không dùng `localhost` khi nhiều node). |

### Replication cho topic nội bộ và mặc định
| Config | Dùng để làm gì |
|---|---|
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | Số bản sao của topic `__consumer_offsets` (lưu offset consumer đã đọc). Mặc định 3 nên 1 broker phải hạ về 1, không thì consumer group không chạy. |
| `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR` / `..._MIN_ISR` | Tương tự cho topic lưu trạng thái Kafka transactions. |
| `KAFKA_DEFAULT_REPLICATION_FACTOR` | RF mặc định cho topic tự tạo / tạo không chỉ định. Mặc định 1. Cluster 3 node đặt 3. |
| `KAFKA_MIN_INSYNC_REPLICAS` | Với `acks=all`, cần tối thiểu bao nhiêu replica (kể cả leader) xác nhận thì ghi mới thành công. RF=3 + min ISR=2: sập 1 broker vẫn ghi được, sập 2 thì ghi bị từ chối (chọn không mất dữ liệu thay vì luôn sẵn sàng). |
| `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS` | Trì hoãn rebalance đầu tiên của consumer group. Đặt 0 để dev khởi động nhanh (mặc định 3000ms). |

### Các thứ phụ trong compose
- `x-kafka-env: &kafka-env` và `<<: *kafka-env`: YAML anchor, dùng chung một khối env cho nhiều node để khỏi lặp.
- `profiles: ["scale"]` (kafka-4): chỉ chạy khi bật `--profile scale`, không chạy theo `up -d` bình thường.
- `depends_on`: chỉ đảm bảo **thứ tự khởi động container**, không đảm bảo Kafka đã sẵn sàng nhận kết nối.
- `kafka-ui`: `KAFKA_CLUSTERS_0_NAME` là tên hiển thị; `BOOTSTRAPSERVERS` phải là địa chỉ **INTERNAL** (`kafka:29092`) vì UI chạy trong container.

## 2. Producer (transaction-service, `application.yml`)
| Config | Dùng để làm gì |
|---|---|
| `bootstrap-servers` | Danh sách broker để lấy metadata ban đầu. Cluster nên liệt kê nhiều node để một node sập vẫn kết nối được. |
| `acks: all` | Leader chỉ báo thành công khi mọi replica **trong ISR** đã ghi. Kết hợp `min.insync.replicas` mới có bảo đảm thật. `1` = chỉ leader; `0` = không chờ gì (có thể mất tin). |
| `enable.idempotence: true` | Producer gắn producer-id + sequence cho từng tin; broker loại bản trùng khi producer retry. Chống duplicate do retry của chính producer (không chống duplicate do logic nghiệp vụ gửi 2 lần). |
| `max.in.flight.requests.per.connection: 5` | Số request chưa được xác nhận cùng lúc. Với idempotence, ≤5 vẫn giữ đúng thứ tự khi retry. |
| `key-serializer` / `value-serializer` | Biến key/value thành byte. Ở đây String (payload là JSON dạng chuỗi). |
| Kafka key = `walletId` (trong `OutboxRelay`) | Cùng key → cùng partition → giữ đúng thứ tự theo từng ví. |

## 3. Consumer (notification-service)
| Config | Dùng để làm gì |
|---|---|
| `group-id` | Các instance cùng group chia nhau partition; mỗi tin chỉ một instance trong group xử lý. Khác group thì mỗi group nhận đủ bản của mình. |
| `auto-offset-reset: earliest` | Khi group chưa có offset (lần đầu) hoặc offset không còn hợp lệ → đọc từ **đầu** topic. `latest` = chỉ đọc tin mới. |
| `enable-auto-commit: false` | Không để client tự commit offset theo giờ (dễ mất/đọc trùng). Để Spring commit sau khi listener xử lý xong. |
| `listener.ack-mode: record` | Commit offset sau **mỗi record** xử lý thành công. (`batch` mặc định: sau cả lô.) Chi tiết hơn nhưng nhiều lần commit hơn. |
| `DefaultErrorHandler` + `FixedBackOff(1000, 3)` | Lỗi xử lý → thử lại 3 lần, cách 1 giây. |
| `DeadLetterPublishingRecoverer` | Hết lần thử vẫn lỗi → đẩy tin sang `<topic>.DLT` rồi đi tiếp, không kẹt cả partition vì một poison message. |
| Bảng `processed_event` + `ON CONFLICT DO NOTHING` | Idempotent consumer: tin đến lần 2 thì insert không thêm dòng → bỏ qua. Cần vì giao hàng là at-least-once. |
| `sql.init.mode: always` | Spring chạy `schema.sql` mỗi lần khởi động để tạo bảng (dùng `IF NOT EXISTS` nên an toàn). |

## 4. Topic (`TopicConfig`)
| Config | Dùng để làm gì |
|---|---|
| `partitions(3)` | Đơn vị song song: tối đa 3 consumer trong 1 group cùng đọc. Chỉ tăng được, không giảm; tăng làm key đổi partition. |
| `replicas(app.kafka.replicas)` | Số bản sao. 1 broker = 1; cluster 3 broker = 3 (đặt `APP_KAFKA_REPLICAS=3`). RF không được lớn hơn số broker. |

## 5. Các service khác
| Config | Dùng để làm gì |
|---|---|
| `spring.cloud.openfeign.client.config.default.connect-timeout: 1000` | Chờ bắt tay TCP tối đa 1s. Ngắn vì kết nối chậm thường nghĩa là service chết. |
| `read-timeout: 3000` | Chờ phản hồi sau khi đã gửi request tối đa 3s. Hết hạn = **không biết** wallet đã xử lý chưa → status `UNKNOWN`. |
| `logger-level: basic` | Log method, URL, status, thời gian của Feign call. |
| `Retryer.NEVER_RETRY` (`FeignConfig`) | Tắt retry mặc định của Feign, vì retry POST chuyển tiền có thể trừ tiền 2 lần. |
| `ErrorDecoder` | Đổi HTTP status thành exception nghiệp vụ (404 → WALLET_NOT_FOUND, 422 → INSUFFICIENT_BALANCE), còn lại là lỗi hạ tầng. |
| `jpa.hibernate.ddl-auto: update` | Hibernate tự tạo/sửa bảng theo entity. Tiện cho dev; production dùng Flyway/Liquibase. |
| `server.port` 8081/8082/8083 | Mỗi service một cổng. |
| `wallet-service.url` | Địa chỉ wallet-service cho Feign. Chưa có Eureka nên khai báo cố định (nâng cấp sau: bỏ `url`, dùng tên service + service discovery). |
| `docker/init-db.sql` | Tạo 3 database `wallet`, `txn`, `notif` khi Postgres khởi động lần đầu (chỉ chạy khi volume còn trống). |
| `POSTGRES_PASSWORD: pass` | Mật khẩu user `postgres`. Chỉ dùng cho dev. |
