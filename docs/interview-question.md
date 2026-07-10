# Bộ Câu Hỏi Phỏng Vấn Backend Engineer (Junior -> Strong Senior/Leader)

Tài liệu này cung cấp danh sách 100 câu hỏi phỏng vấn chuyên sâu dành cho vị trí Backend Engineer, tập trung vào các công nghệ: **Kafka, Kubernetes, Redis, Elasticsearch, REST/gRPC, Quarkus, DynamoDB, AWS Bedrock (AI/LLM), System Design & Leadership**.

Mỗi câu hỏi đi kèm với mức độ và hướng dẫn trả lời/key points để đánh giá ứng viên.

---

## Mục Lục
1. [Apache Kafka](#1-apache-kafka-1-12)
2. [Kubernetes (K8s)](#2-kubernetes-k8s-13-24)
3. [Redis](#3-redis-25-36)
4. [Elasticsearch](#4-elasticsearch-37-48)
5. [REST API & gRPC](#5-rest-api--grpc-49-60)
6. [Quarkus & Java Backend](#6-quarkus--java-backend-61-70)
7. [DynamoDB](#7-dynamodb-71-80)
8. [AWS Bedrock & GenAI](#8-aws-bedrock--genai-81-90)
9. [System Design, Problem Solving & Leadership](#9-system-design-problem-solving--leadership-91-100)

---

## 1. Apache Kafka (1-12)

**1. (Junior) Kafka là gì? Khác biệt cơ bản giữa Kafka và RabbitMQ?**
- **Trả lời:** Kafka là hệ thống event-streaming phân tán, lưu data dưới dạng append-only log. RabbitMQ là message broker truyền thống (Smart broker, dumb consumer). Kafka giữ message sau khi consume (tùy retention policy), RabbitMQ xóa sau khi ack. Kafka phù hợp cho high throughput/streaming, RabbitMQ hợp cho routing phức tạp/task queue.

**2. (Junior) Giải thích khái niệm Topic, Partition, Offset trong Kafka?**
- **Trả lời:** **Topic**: Phân loại logic của message. **Partition**: Topic chia thành nhiều partition (giúp scale, concurrent). **Offset**: Số thứ tự định danh message trong 1 partition, consumer dùng offset để track vị trí đã đọc.

**3. (Mid) Consumer Group hoạt động như thế nào? Chuyện gì xảy ra khi thêm/bớt consumer?**
- **Trả lời:** Nhiều consumer cùng chung `group.id`. Mỗi partition trong topic chỉ được assign cho TỐI ĐA 1 consumer trong 1 group. Khi thêm/bớt, xảy ra quá trình **Rebalance** (dừng đọc, tính toán lại assignment). Ứng viên tốt sẽ nhắc đến chi phí của Rebalance và Static Membership để giảm thiểu nó.

**4. (Mid) Làm sao để đảm bảo message order (thứ tự) trong Kafka?**
- **Trả lời:** Kafka chỉ đảm bảo order trong **cùng 1 partition**. Để order toàn cục, cần cho vào 1 partition (giảm throughput). Hoặc dùng Message Key (hashing cùng 1 user_id vào cùng 1 partition).

**5. (Mid) At-most-once, At-least-once, Exactly-once trong Kafka là gì?**
- **Trả lời:** Semantic giao nhận message. Mặc định là At-least-once (có thể lặp). Để đạt **Exactly-once**, cần bật Idempotent Producer (`enable.idempotence=true`) và dùng Transaction API, phía consumer chỉnh `isolation.level=read_committed`.

**6. (Senior) Cách xử lý tình trạng lag của Consumer?**
- **Trả lời:** 1) Scale up consumer (nếu còn partition trống). 2) Tăng partition của topic. 3) Tối ưu code xử lý ở consumer (dùng async/batch processing). 4) Kiểm tra lỗi (Poison pill) gây kẹt offset.

**7. (Senior) Giải thích cơ chế Replication và In-Sync Replicas (ISR) trong Kafka.**
- **Trả lời:** Mỗi partition có 1 Leader và nhiều Follower. ISR là danh sách các replicas bắt kịp data với leader. Nếu leader chết, 1 follower trong ISR sẽ lên làm leader. Cấu hình `min.insync.replicas` và `acks=all` để đảm bảo không mất data.

**8. (Senior) Split-brain trong Kafka là gì và cách giải quyết?**
- **Trả lời:** Tình trạng 2 broker cùng nghĩ mình là leader do lỗi mạng. Kafka dùng Zookeeper (hoặc KRaft quorum mới) thuật toán consensus (Raft) với số node lẻ (3,5) để vote. Node nào không thuộc majority sẽ bị loại.

**9. (Senior) Làm sao để tối ưu throughput trong Kafka Producer?**
- **Trả lời:** Tăng `batch.size` và `linger.ms` (đợi gom nhiều message gửi 1 lần). Bật compression (Snappy/LZ4). Dùng `acks=1` (hy sinh chút độ an toàn).

**10. (Senior) Xử lý Poison Pill (message lỗi gây crash logic) như thế nào?**
- **Trả lời:** Không để consumer crash liên tục (gây infinite loop). Cần Catch Exception, log lại, gửi message lỗi sang **Dead Letter Queue (DLQ)**, và commit offset để đi tiếp.

**11. (Senior/Lead) Thiết kế hệ thống Retry với Kafka?**
- **Trả lời:** Khác RabbitMQ, Kafka không có sẵn delay retry. Thường tạo các topic retry với delay tăng dần (Retry-5m, Retry-1h) hoặc kết hợp với 1 database/scheduler bên ngoài để trigger lại.

**12. (Senior/Lead) Bạn đã bao giờ gặp vấn đề mất data (Data Loss) trong Kafka chưa? Nguyên nhân và cách phòng tránh?**
- **Trả lời:** Nguyên nhân: `acks=0` hoặc `1` (leader chết chưa kịp sync), auto-commit bật nhưng logic fail, unclean leader election bật. Phòng tránh: `acks=all`, `min.insync.replicas >= 2`, tắt auto-commit, commit thủ công (sync/async) sau khi process xong.

---

## 2. Kubernetes (K8s) (13-24)

**13. (Junior) Pod, Node, Cluster là gì?**
- **Trả lời:** **Pod**: Đơn vị nhỏ nhất, chứa 1 hoặc nhiều container chung network/storage. **Node**: Máy ảo/vật lý chạy các Pod. **Cluster**: Tập hợp các Node và Control Plane.

**14. (Junior) Sự khác biệt giữa Deployment và StatefulSet?**
- **Trả lời:** Deployment dùng cho Stateless app (pod bị diệt tạo lại tên ngẫu nhiên). StatefulSet cho Stateful app (DB, Kafka), pod có tên cố định (pod-0, pod-1), thứ tự khởi động/tắt được bảo đảm, network identity cố định.

**15. (Mid) ClusterIP, NodePort, LoadBalancer khác nhau ra sao?**
- **Trả lời:** **ClusterIP**: Chỉ gọi được nội bộ cluster. **NodePort**: Mở port trên mọi Node ra ngoài. **LoadBalancer**: Yêu cầu cloud provider cấp 1 Public IP/LB để trỏ vào cluster.

**16. (Mid) Ingress là gì? Khác gì với Service LoadBalancer?**
- **Trả lời:** Ingress là object quản lý external access vào cluster bằng HTTP/HTTPS routing (path/domain based), có SSL termination. Thay vì tốn nhiều LoadBalancer cho nhiều service, 1 Ingress Controller (như Nginx) gom lại tiết kiệm chi phí.

**17. (Mid) ConfigMap và Secret? Best practice dùng Secret?**
- **Trả lời:** Lưu cấu hình và dữ liệu nhạy cảm. Lưu ý: Secret mặc định chỉ encode Base64, KHÔNG mã hóa (encryption). Cần dùng RBAC, KMS provider hoặc các tool như HashiCorp Vault, External Secrets Operator.

**18. (Mid) Liveness Probe và Readiness Probe?**
- **Trả lời:** **Liveness**: Pod còn sống không? Chết thì K8s restart. **Readiness**: Pod sẵn sàng nhận traffic chưa? Trả false thì K8s gỡ khỏi Service endpoint (không nhận request), nhưng không restart.

**19. (Senior) HPA hoạt động dựa trên metric nào? Cấu hình Custom Metric?**
- **Trả lời:** Mặc định dựa trên CPU/Memory qua Metrics Server. Để dùng metric ngoài (ví dụ Queue length từ Kafka/Redis), cần cài KEDA hoặc Prometheus Adapter.

**20. (Senior) Quá trình K8s schedule 1 Pod? (Taints/Tolerations, Affinity)**
- **Trả lời:** Kube-scheduler lọc (filter) các node phù hợp và chấm điểm (score). **Taints/Tolerations**: Đẩy Pod khỏi Node (Node nói "đừng vào tôi nếu không có toleration"). **Affinity/Anti-affinity**: Thu hút Pod vào Node (Pod nói "tôi muốn ở node có nhãn X" hoặc "tránh xa pod Y").

**21. (Senior) Zero-downtime deployment & Graceful shutdown?**
- **Trả lời:** Dùng RollingUpdate (maxUnavailable=0). Ứng dụng phải handle SIGTERM, hoàn thành các request đang dở, kết hợp với preStop hook và Readiness probe chuẩn xác.

**22. (Senior) Resource Requests và Limits? Chuyện gì xảy ra nếu vượt Limit?**
- **Trả lời:** Requests: K8s dùng để tìm Node có đủ tài nguyên. Limits: Ngưỡng tối đa. Vượt RAM Limit -> Pod bị **OOMKilled**. Vượt CPU Limit -> Bị **CPU Throttling** (chậm đi, không chết).

**23. (Senior/Lead) Xử lý khi 1 node bị crash (Node failure)?**
- **Trả lời:** Kubelet ngừng gửi heartbeat, Control Plane đánh dấu NotReady. Pod bị Evict (chuyển sang node khác) sau 5 phút (pod-eviction-timeout). Để high availability, app cần replica >= 2 và trải đều qua PodAntiAffinity.

**24. (Senior/Lead) Chiến lược chạy Database trên K8s? Có nên không?**
- **Trả lời:** Có thể nhưng phức tạp. Cần dùng StatefulSet, Persistent Volumes (CSI), và DB Operator (ví dụ Zalando Postgres Operator) để auto backup/failover. Nếu team chưa cứng K8s, nên dùng Managed DB (RDS/Aurora) bên ngoài.

---

## 3. Redis (25-36)

**25. (Junior) Tại sao Redis lại cực nhanh?**
- **Trả lời:** In-memory DB, cấu trúc dữ liệu tối ưu (C), Single-threaded (tránh context switching và lock contention), dùng Multiplexing I/O.

**26. (Junior) Các kiểu dữ liệu cơ bản?**
- **Trả lời:** String, List, Set, Hash, Sorted Set (ZSET).

**27. (Mid) Redis persistence: RDB vs AOF?**
- **Trả lời:** **RDB**: Snapshot theo thời gian định kỳ (nhỏ gọn, phục hồi nhanh, có thể mất data phút cuối). **AOF**: Log từng lệnh write (to hơn, phục hồi chậm, an toàn hơn). Thường bật cả hai.

**28. (Mid) Cache Penetration, Cache Breakdown, Cache Avalanche?**
- **Trả lời:**
    - **Penetration**: Query key không tồn tại ở Cache & DB liên tục -> Dùng Bloom Filter hoặc cache Null value.
    - **Breakdown**: 1 Hot key hết hạn, hàng ngàn request đập vào DB -> Dùng Mutex Lock (chỉ 1 request được lấy DB cập nhật cache).
    - **Avalanche**: Hàng loạt key hết hạn cùng lúc -> Thêm random jitter vào TTL.

**29. (Mid) Cơ chế Eviction khi đầy RAM?**
- **Trả lời:** LRU (Least Recently Used), LFU (Least Frequently Used), Random, TTL. Nếu set `noeviction`, Redis trả lỗi khi write.

**30. (Mid) Pub/Sub trong Redis khác gì Kafka?**
- **Trả lời:** Fire-and-forget. Không lưu trữ, consumer offline lúc pub sẽ mất tin nhắn. Kafka có lưu trữ, offset, replay được.

**31. (Senior) Redis Transaction (MULTI/EXEC)?**
- **Trả lời:** Gom lệnh chạy tuần tự, nhưng **không có Rollback** tự động nếu 1 lệnh bị lỗi cú pháp lúc run. Phải dùng WATCH để đảm bảo Optimistic Locking.

**32. (Senior) LUA Script trong Redis? Ưu điểm?**
- **Trả lời:** Chạy script atomic trên server. Thay vì fetch data về app -> tính toán -> ghi lại (gây race condition), Lua thực thi tất cả trong 1 step duy nhất trên Redis, tránh network round-trip.

**33. (Senior) Redis Sentinel vs Redis Cluster?**
- **Trả lời:** **Sentinel**: High Availability, theo dõi và promote replica lên master nếu master chết. Phù hợp data vừa phải. **Cluster**: Cả HA và Sharding. Dữ liệu chia vào 16384 hash slots trên nhiều master nodes.

**34. (Senior) Xử lý concurrency issues (Distributed Lock)?**
- **Trả lời:** Dùng `SET key value NX PX 3000`. Cẩn thận lỗi: 1) App xử lý chậm hơn 3s, key hết hạn, thread khác lấy lock -> Thread 1 xong xóa nhầm lock của thread 2 (cần check value UUID bằng Lua script trước khi DEL). Tốt nhất dùng thư viện chuẩn (Redlock/Redisson).

**35. (Senior/Lead) Làm sao để scale Redis khi dữ liệu quá lớn?**
- **Trả lời:** Client-side sharding, Proxy-based sharding (Twemproxy/Envoy), hoặc Redis Cluster. Chú ý các lệnh multi-key bị giới hạn nếu keys không cùng hash slot (dùng `{hash tag}`).

**36. (Senior/Lead) Kinh nghiệm debug Redis bị High Latency đột ngột?**
- **Trả lời:** Kiểm tra `SLOWLOG` (có query `KEYS *` hay `HGETALL` data lớn không). Kiểm tra `INFO memory` xem có swap không. Kiểm tra Transparent Huge Pages (THP) của Linux, độ trễ fork() khi lưu RDB nền (bgsave).

---

## 4. Elasticsearch (37-48)

**37. (Junior) Inverted Index là gì?**
- **Trả lời:** Thay vì map Document -> Words, Inverted Index map Word -> List các Document chứa word đó. Giúp full-text search siêu tốc.

**38. (Junior) Index, Document, Type?**
- **Trả lời:** Index ~ Database/Table, Document ~ Row. Khái niệm Type đã bị loại bỏ từ ES 7.x.

**39. (Mid) Node roles (Master, Data, Ingest)?**
- **Trả lời:** Master: Quản lý metadata/cluster state. Data: Chứa data và thực hiện search. Ingest: Pre-process document trước khi index (như Logstash mini). Coordinating: Nhận request và route đến node chứa data.

**40. (Mid) Mapping? Dynamic mapping có rủi ro gì?**
- **Trả lời:** Định nghĩa schema. Dynamic tự đoán kiểu (ví dụ đoán string thành `text`), làm tốn bộ nhớ nếu không cần full-text, hoặc lỗi mapping explosion (quá nhiều fields).

**41. (Mid) Text vs Keyword datatype?**
- **Trả lời:** `text`: Qua Analyzer (chia từ, lowercase) dùng cho full-text search. `keyword`: Giữ nguyên string chính xác, dùng cho filter, sort, aggregation.

**42. (Mid) Query context vs Filter context?**
- **Trả lời:** Query: Tính điểm relevance score (_score) xem mức độ phù hợp. Filter: Chỉ trả lời Yes/No, KHÔNG tính điểm, có thể được **cache** -> cực nhanh.

**43. (Senior) Shards và Replicas? Chọn số lượng ban đầu?**
- **Trả lời:** Shard là 1 Lucene index. Không thể đổi số Primary Shard sau khi tạo index (trừ Reindex/Split). Rule of thumb: Giữ shard size từ 10GB - 50GB.

**44. (Senior) Write path diễn ra thế nào?**
- **Trả lời:** Request -> Coordinating Node -> Primary Shard -> Execute index -> Replicate sang Replicas -> Trả kết quả (tuỳ vào write consistency). Dữ liệu vào memory buffer & translog -> Refresh (searchable) -> Flush (lưu disk).

**45. (Senior) Read path (Scatter/Gather)?**
- **Trả lời:** Query search 10 kết quả -> Coordinating gửi yêu cầu tới TẤT CẢ shards -> mỗi shard trả về top 10 (chỉ lấy doc_id & score) -> Coordinating merge/sort lại lấy ra top 10 cuối cùng -> Fetch data thật từ shard về trả user. Cảnh báo: Deep pagination quá lớn sẽ sập RAM.

**46. (Senior) Update 1 field trong document?**
- **Trả lời:** Document trong ES là immutable. Lệnh update thực chất là: Đọc doc cũ -> thay đổi -> Index thành doc mới -> Đánh dấu doc cũ là deleted. Tốn kém IO.

**47. (Senior/Lead) Giải quyết Split-brain trong ES?**
- **Trả lời:** ES >= 7.x tự quản lý cluster state, thuật toán thay đổi. Trước đó cần cấu hình `discovery.zen.minimum_master_nodes = (master_eligible_nodes / 2) + 1`.

**48. (Senior/Lead) Quản lý Index cho Time-series data (Log)?**
- **Trả lời:** Dùng ILM (Index Lifecycle Management). Chuyển data từ Hot -> Warm -> Cold node. Dùng Rollover API để tự cắt index mới khi đạt ngưỡng size (vd: 50GB) hoặc ngày tuổi. Data cũ có thể freeze hoặc delete.

---

## 5. REST API & gRPC (49-60)

**49. (Junior) Các HTTP Methods khác nhau thế nào? Idempotency?**
- **Trả lời:** GET (đọc), POST (tạo mới), PUT (cập nhật đè toàn bộ), PATCH (cập nhật 1 phần), DELETE (xóa). Idempotent (gọi nhiều lần kết quả server state vẫn như 1): GET, PUT, DELETE. POST không idempotent.

**50. (Junior) RESTful API là gì?**
- **Trả lời:** Kiến trúc web dùng HTTP chuẩn, resource-based (URL mô tả danh từ), stateless, dùng HTTP status codes (2xx, 4xx, 5xx).

**51. (Junior) gRPC là gì? Protocol Buffers (Protobuf)?**
- **Trả lời:** Framework RPC của Google. Dùng HTTP/2, payload binary qua Protobuf (nhỏ gọn, strongly typed, gen code tự động) thay vì JSON text cồng kềnh.

**52. (Mid) gRPC vs REST? Khi nào dùng?**
- **Trả lời:** Dùng gRPC cho giao tiếp nội bộ (Server-to-Server/Microservices) vì tốc độ cao, strict schema. Dùng REST cho public API (Web/Mobile) vì dễ parse, dễ debug, hệ sinh thái rộng.

**53. (Mid) Các loại streaming trong gRPC?**
- **Trả lời:** Unary (1 request-1 response), Server streaming (1 req - N res), Client streaming (N req - 1 res), Bi-directional streaming (N req - N res, full duplex).

**54. (Mid) API Versioning?**
- **Trả lời:** REST: Bằng URI (`/v1/users`), Header (`Accept-version`), Query param. gRPC: Versioning nằm trong package của file `.proto` (VD: `package user.v1;`). Không sửa/xóa field cũ, chỉ thêm field mới.

**55. (Senior) Authentication trong Microservices?**
- **Trả lời:** Dùng API Gateway (xác thực token) -> chuyển thành header nội bộ (X-User-Id) hoặc JWT truyền xuống internal service. Stateless JWT giúp service tự verify chữ ký (RS256) không cần chọc DB.

**56. (Senior) Thuật toán Rate Limiting?**
- **Trả lời:**
    - Token Bucket: Bình chứa token, request lấy token. Đầy đặn mượt.
    - Fixed window: Đếm request mỗi giây (dễ bị burst ở chuyển giao giây).
    - Sliding window log/counter: Mượt nhưng tốn bộ nhớ. Có thể implement qua Redis.

**57. (Senior) Pagination hàng triệu records?**
- **Trả lời:** Offset pagination (`OFFSET 1000000`) rẽ quét qua 1tr dòng rồi bỏ, rất chậm. Dùng **Cursor-based pagination** (`WHERE id > last_id LIMIT 10`) kết hợp index, độ phức tạp O(1).

**58. (Senior) GraphQL khác gì REST?**
- **Trả lời:** Client tự định nghĩa data muốn lấy, tránh Over-fetching / Under-fetching. Chỉ có 1 endpoint POST. Điểm yếu: Khó cache HTTP (CDN), query phức tạp có thể DDoS server (cần depth limit, query cost).

**59. (Senior/Lead) Thiết kế API Gateway? Bottleneck?**
- **Trả lời:** Đảm nhiệm Auth, Rate Limit, Routing, SSL, Tracing. Bottleneck xảy ra nếu Gateway xử lý logic business hoặc payload quá nặng. Nên dùng các giải pháp async/non-blocking (Kong, Nginx, Envoy, Spring Cloud Gateway).

**60. (Senior/Lead) Debug/Tracing request qua nhiều services?**
- **Trả lời:** Dùng Distributed Tracing (OpenTelemetry, Jaeger). Sinh ra TraceId ở Gateway, truyền xuyên suốt qua HTTP Headers hoặc gRPC Metadata. Đóng gói log kèm TraceId để search trên Kibana/Datadog.

---

## 6. Quarkus & Java Backend (61-70)

**61. (Junior) Quarkus khác gì Spring Boot?**
- **Trả lời:** Quarkus build tối ưu cho container/Cloud Native. Khởi động tính bằng mili-giây, tốn rất ít RAM (so với Spring Boot), nhờ đẩy nhiều process (DI/reflection) sang Build-time.

**62. (Junior) Container-first framework?**
- **Trả lời:** Sinh ra để chạy trong K8s. Nghĩa là footprint nhỏ gọn, scale-up nhanh, và có thể biên dịch ra file thực thi native (không cần JVM).

**63. (Mid) Native compilation (GraalVM)? Lợi & Hại?**
- **Trả lời:** Biên dịch Java ra mã máy OS (AOT - Ahead of Time). **Lợi**: Boot cực nhanh, RAM siêu nhỏ. **Hại**: Build chậm (tốn tài nguyên), khó debug native, hạn chế dùng Reflection động.

**64. (Mid) Reactive vs Imperative trong Quarkus?**
- **Trả lời:** Hỗ trợ song song 2 mô hình. Dựa trên lõi Vert.x. Imperative: Block thread, code dễ đọc. Reactive (Mutiny): Non-blocking, dùng ít thread, scale cực mạnh xử lý I/O.

**65. (Mid) CDI/Arc hoạt động thế nào?**
- **Trả lời:** Khác Spring dùng Reflection rà quét annotation lúc Runtime, Quarkus Arc làm việc này lúc Build-time, sinh ra byte-code khởi tạo sẵn. Giảm time boot và RAM.

**66. (Senior) Quarkus tối ưu bộ nhớ như thế nào?**
- **Trả lời:** Dead Code Elimination (GraalVM), Class pre-initialization, và di dời logic parsing config, scanning annotation từ runtime sang build-time.

**67. (Senior) Hibernate ORM with Panache?**
- **Trả lời:** Wrapper quanh Hibernate, mang phong cách Active Record. Giảm thiểu boilerplate code (Repository/Entity). Hỗ trợ Reactive Hibernate Panache để không block DB threads.

**68. (Senior) Xử lý Thread Pool (Event Loop) trong mô hình Reactive?**
- **Trả lời:** Có Event Loop threads (chuyên nhận event/IO, cực ít, không được block) và Worker threads (cho blocking I/O). Code Quarkus tự detect annotation `@Blocking` hoặc `@NonBlocking` để dispatch sang đúng thread pool, nếu lỡ block Event Loop app sẽ crash/báo lỗi.

**69. (Senior/Lead) Migrate Spring Boot sang Quarkus?**
- **Trả lời:** Quarkus có Spring Compatibility Extensions (hỗ trợ `@RestController`, `@Autowired`). Nhưng để tối ưu, cần đập đi viết lại theo chuẩn CDI/JAX-RS. Khó nhất là thư viện third-party dùng reflection chưa tương thích với GraalVM native.

**70. (Senior/Lead) Profiling performance Quarkus Native?**
- **Trả lời:** JDK Flight Recorder / VisualVM khó chạy trên Native. Cần dùng perf, eBPF hoặc các tool C-level. Thường profile và tinh chỉnh trên JVM mode, sau đó verify trên Native.

---

## 7. DynamoDB (71-80)

**71. (Junior) DynamoDB là gì? NoSQL khác gì SQL?**
- **Trả lời:** Fully managed NoSQL key-value/document DB của AWS. Không có Schema cứng, scale vô hạn, không có tính năng JOIN phức tạp.

**72. (Junior) Partition Key và Sort Key?**
- **Trả lời:** Khóa chính (Primary Key). **Partition Key (PK)** xác định node vật lý lưu trữ data (phải phân tán đều). **Sort Key (SK)** (tùy chọn) phân loại và sort data trong cùng 1 PK.

**73. (Mid) RCU và WCU? Provisioned vs On-demand?**
- **Trả lời:** Đơn vị đo Capacity (Read/Write Capacity Unit). Provisioned: Thuê trước lượng capacity cố định (rẻ hơn, dễ bị throtle nếu burst). On-demand: Trả tiền theo request thực tế (đắt hơn, tự scale tự động).

**74. (Mid) LSI và GSI?**
- **Trả lời:** **LSI**: Cùng Partition key, khác Sort key. Phải tạo lúc lập table. Dùng chung RCU/WCU của table chính. **GSI**: Khác Partition key. Tạo lúc nào cũng được, cấu hình RCU/WCU riêng, là 1 index bảng riêng đồng bộ bất đồng bộ.

**75. (Mid) Eventual vs Strongly consistent reads?**
- **Trả lời:** Eventual (mặc định): Nhanh, rẻ (tốn 0.5 RCU), nhưng có thể đọc số liệu cũ vừa cập nhật. Strongly: Đảm bảo đọc data mới nhất, chậm hơn, đắt (tốn 1 RCU). GSI chỉ hỗ trợ Eventual.

**76. (Senior) Hot Partition là gì?**
- **Trả lời:** Một Partition Key (vd `status=ACTIVE`) có quá nhiều request đập vào, gây vượt ngưỡng phần cứng của node đó (Throttling) dù tổng capacity chưa hết. Cách giải quyết: Thêm UUID/Suffix vào khóa (Write sharding).

**77. (Senior) Single-Table Design?**
- **Trả lời:** Lưu tất cả Entities (User, Order, Item) vào MỘT bảng duy nhất dùng chung PK, SK. Lợi ích: Lấy toàn bộ data liên quan trong 1 query rẻ tiền (tránh JOIN). Hạn chế: Khó phân tích data (Analytics), khó thay đổi access pattern sau này.

**78. (Senior) DynamoDB Streams? Use case (CDC)?**
- **Trả lời:** Ghi nhận mọi sự thay đổi (Insert/Update/Delete) theo thời gian thực (time-ordered). Use case: Trigger AWS Lambda để update Elasticsearch, gửi thông báo, cache invalidation, Data Replication.

**79. (Senior/Lead) Xử lý TTL (Time to Live)?**
- **Trả lời:** Đánh dấu attribute timestamp hết hạn, AWS sẽ tự động xóa nền ngầm. Ưu điểm: KHÔNG tốn WCU cho việc xóa. Lưu ý: Thời gian xóa không chính xác lập tức (có thể mất tối đa 48h sau TTL).

**80. (Senior/Lead) Kiến trúc Full-text search trên DynamoDB?**
- **Trả lời:** DynamoDB không sinh ra để search `%LIKE%`. Cần bật DynamoDB Streams -> Kích hoạt Lambda -> Đẩy data sang Elasticsearch (OpenSearch). User query search trên ES, lấy ID rồi get data gốc từ DynamoDB (nếu cần).

---

## 8. AWS Bedrock & GenAI (81-90)

**81. (Junior) AWS Bedrock là gì?**
- **Trả lời:** Dịch vụ Serverless của AWS cung cấp API truy cập các foundation models (Claude, Llama, Titan...) qua 1 API chuẩn mà không cần quản lý hạ tầng hay GPU.

**82. (Junior) Prompt Engineering cơ bản?**
- **Trả lời:** Kỹ năng viết chỉ thị cho LLM. Cần rõ ràng (Role, Context, Task, Format), dùng few-shot (đưa ví dụ), Chain-of-Thought (bảo AI suy nghĩ từng bước).

**83. (Mid) RAG (Retrieval-Augmented Generation) là gì?**
- **Trả lời:** Đưa thêm document/context doanh nghiệp (private data) vào Prompt để LLM trả lời dựa trên đó thay vì kiến thức học được. Rẻ, cập nhật nhanh và ít bịa đặt (hallucination) hơn so với Fine-tuning.

**84. (Mid) Embeddings và Vector Database?**
- **Trả lời:** Embeddings là chuyển đổi text thành mảng vector số thực, thể hiện ý nghĩa ngữ nghĩa. Vector DB (Pinecone, pgvector) lưu vector này và hỗ trợ search "Cosine Similarity" để tìm các đoạn text gần nghĩa với câu hỏi user.

**85. (Mid) Temperature, Top-P?**
- **Trả lời:** Tham số kiểm soát độ sáng tạo. Temperature thấp (0): Câu trả lời logic, cố định, chính xác. Temperature cao (0.8-1): Sáng tạo, thay đổi liên tục. Cần set = 0 cho các task phân tích dữ liệu, RAG.

**86. (Senior) Kiến trúc RAG chuẩn trên AWS?**
- **Trả lời:** User Question -> Amazon Titan Embedding Model -> Vectorize -> Search trên OpenSearch Serverless (Vector search) -> Lấy Top 5 docs -> Gom Question + Docs tạo Prompt -> Gọi Claude 3 (Bedrock) -> Trả kết quả.

**87. (Senior) Xử lý Context Window Limit?**
- **Trả lời:** Khi text quá dài, cần Chunking (cắt nhỏ document với overlap) trước khi nhúng. Trong hội thoại, có thể dùng 1 LLM tóm tắt bớt lịch sử chat (Conversation Summary Buffer) trước khi nhét vào prompt.

**88. (Senior) Hallucination và cách giảm thiểu?**
- **Trả lời:** AI tự bịa thông tin. Giảm bằng: System Prompt chặt chẽ ("Chỉ trả lời dựa trên tài liệu được cấp, nếu không có hãy nói Không biết"), dùng RAG xịn, và Evaluation checks.

**89. (Senior/Lead) Đánh giá chất lượng và Monitor cost?**
- **Trả lời:** Dùng "LLM as a judge" (Dùng 1 model xịn như GPT-4/Claude 3.5 Sonnet để chấm điểm câu trả lời RAG của model nhỏ hơn). Đo lường input/output tokens. Bedrock hỗ trợ Provisioned Throughput để cố định cost nếu scale lớn.

**90. (Senior/Lead) Agentic Workflow & Function Calling (Tool use)?**
- **Trả lời:** Khả năng LLM tự phân tích câu hỏi và sinh ra JSON yêu cầu gọi một API bên thứ 3 (ví dụ API check tồn kho). Backend sẽ thực thi API đó, lấy JSON kết quả trả lại cho LLM để LLM tổng hợp ra câu trả lời cuối cùng cho User. AWS Bedrock hỗ trợ Agent tự động orchestrate việc này.

---

## 9. System Design, Problem Solving & Leadership (91-100)

**91. (Senior) CQRS và Event Sourcing?**
- **Trả lời:** CQRS tách Command (Write) và Query (Read) sang 2 database/model khác nhau, đồng bộ qua event. Event Sourcing lưu mọi thay đổi dưới dạng chuỗi Event bất biến, trạng thái cuối là kết quả tính tổng các event. Áp dụng: Hệ thống core banking, order history.

**92. (Senior) Saga Pattern (Distributed Transaction)?**
- **Trả lời:** Microservices không có ACID bao trùm. Saga giải quyết bằng 1 chuỗi local transaction. Có 2 dạng: **Choreography** (Pub/Sub tự phản ứng) và **Orchestration** (Có 1 central coordinator điều phối). Luôn cần implement "Compensating action" (lệnh hoàn tác) khi 1 bước ở giữa thất bại.

**93. (Senior) Xử lý Idempotency trong Payment?**
- **Trả lời:** Đề phòng rớt mạng, client gửi retry chuyển tiền 2 lần. Phải có Idempotency-Key (UUID) sinh từ Client, backend lưu vào Redis/DB khóa duy nhất này. Nhận request 2 thấy trùng key -> Trả lại response cũ, không trừ tiền lần 2.

**94. (Senior) Microservices vs Monolith vs Modular Monolith?**
- **Trả lời:** Tránh cuồng Microservices. Mới làm nên chọn Modular Monolith (code chung repo/db nhưng tách package rõ ràng). Microservices chỉ khi team lớn, cần scale độc lập, chấp nhận nỗi đau về network latency, DevOps, data consistency.

**95. (Senior/Lead) Database Sharding vs Replication?**
- **Trả lời:** Replication (nhân bản): Scale READ, tăng HA, cùng 1 data ở nhiều máy. Sharding (phân mảnh): Scale cả WRITE/STORAGE, dữ liệu chia dọc/ngang ra nhiều máy, query cross-shard rất chậm/khó.

**96. (Senior/Lead) Incident Response: Lỗi 502/504 hàng loạt nửa đêm?**
- **Trả lời:** Quy trình: 1) Acknowledge alert, lập War room (Meet/Slack). 2) Triage: Rollback deployment gần nhất hoặc scale up tạm để stop-bleeding (cầm máu). 3) Investigate: Check logs, APM, DB CPU, Network. 4) Post-mortem: Viết báo cáo RCA (Root Cause Analysis) và Action items để không tái phạm. Không đổ lỗi.

**97. (Senior/Lead) Thiết kế Notification System (10 triệu users)?**
- **Trả lời:** Dùng Pub/Sub (Kafka). API nhận request (nhanh) -> Đẩy vào Topic. Các worker pool pull job ra, gọi external APIs (FCM/APNS/SendGrid). Cần có Rate Limit bên thứ 3, queue Retry (Dead letter queue) khi SMS gateway lỗi, xử lý template caching ở Redis, và chia Shard/Partition Kafka theo Device_ID/User_ID để parallel process.

**98. (Senior/Lead) Xử lý thành viên team làm việc không hiệu quả?**
- **Trả lời:** Lắng nghe trước (1-on-1 xem họ có vấn đề cá nhân/thiếu tools/thiếu kỹ năng không). Đặt mục tiêu SMART nhỏ dần, pair-programming để mentor. Nếu do thái độ/toxic -> Cảnh cáo và escalate nhân sự (PIP). Quan trọng: Hỗ trợ nhưng không làm thay.

**99. (Senior/Lead) Điểm quan trọng khi review System Architecture (Tech Spec)?**
- **Trả lời:** Focus vào "Non-functional requirements": Scalability (tải X10 chịu được không?), High Availability (Single Point of Failure ở đâu?), Security, Observability (Monitor thế nào?), Database design (đủ scale không?), và Cập nhật có backward compatibility không. Tránh chê bai, đặt câu hỏi "Chuyện gì xảy ra nếu...?".

**100. (Senior/Lead) Kiến trúc sai lầm lớn nhất bạn từng thiết kế và bài học?**
- **Trả lời:** (Câu hỏi mở đánh giá sự trưởng thành). Ứng viên tốt sẽ dám kể lỗi: Áp dụng công nghệ quá lố (Over-engineering), tách Microservices quá sớm dẫn đến rối rắm, hay thiết kế sai Partition Key của NoSQL gây tốn tiền đập đi xây lại. Nhấn mạnh vào "bài học rút ra" (vd: KISS principle, Benchmark kỹ trước khi quyết định).
