# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

- **Họ tên / MSSV:** Nguyễn Trung Kiên / 2A202602764
- **Repo:** https://github.com/kien3007/K4-Track02-Day17-NguyenTrungKien-2A202602764-DataPipelineEngineering
- **Commit bài nộp:** fbf74549564cf474f0b59b1452db9dd4b07cd131
- **AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):**  Hỗ trợ phát hiện lỗi cú pháp cross-platform của Makefile trên Windows và hỗ trợ viết REPORT.md đúng format
- **Nguồn tham khảo khác (nếu có):** Slide bài giảng Day 17, DuckDB documentation, dbt microbatch documentation.

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` có 24 hàng cho 12 tickets; T-91 có 3 trạng thái; `test_silver_tickets_one_row_per_ticket` fail (`assert 24 == 12`). | `test_late_events_land_in_their_event_day` fail (`(2, 1, 0) != (5, 3, 1)`); re-run ngày cũ đổi checksum gold_feature_daily (`c50b8851affe != 8630e04a61d1`). | `test_cdc_delete_becomes_tombstone` fail; T-97 đã xoá vẫn còn trong `silver_tickets` (is_deleted=False), training set snapshot mới nhất và `gold_doc_chunks`. |
| **Nguyên nhân gốc** | `upsert_silver_tickets` chỉ dùng `INSERT INTO`, không deduplicate hay merge theo khoá `ticket_id` giữa các batch, không kiểm tra thứ tự LSN. | `LOOKBACK_DAYS = 0` trong `pipeline/config.py` khiến cửa sổ tính toán feature chỉ quét ngày ingest hiện tại, bỏ sót các sự kiện trễ (P99 lateness = 3 ngày) xảy ra trước đó. | `ticket_changes_sql` trong `pipeline/staging.py` chỉ lấy `ticket_id` từ `j->'value'->'after'`, khi `_op = 'd'` thì `after` là `null` nên `ticket_id` bị `null` và bị lọc mất (`WHERE ticket_id IS NOT NULL`). |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: Dùng `MERGE INTO silver_tickets AS t USING _latest_changes AS s ON t.ticket_id = s.ticket_id WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...`. | `pipeline/config.py`: Đổi `LOOKBACK_DAYS = 3` (bằng $\lceil P99 \rceil$ đo từ Bronze) để cửa sổ overwrite-partition quét lùi đủ 3 ngày `[day - 3, day]`. | `pipeline/staging.py`: Dùng `coalesce(j->'value'->'after'->>'ticket_id', j->'value'->'before'->>'ticket_id', j->'key'->>'ticket_id') AS ticket_id`. |
| **Khái niệm trên slide** | Idempotency & Entity Resolution (MERGE on key, LSN ordering, replay-safe). | Event Time vs Ingestion Time & Handling Late Data (Watermark / Lookback window, Overwrite-partition). | CDC Semantics & Tombstone Propagation (Debezium `op='d'`, soft delete, GDPR/Right to be forgotten). |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: MERGE bảo đảm mỗi ticket chỉ có duy nhất một bản ghi trạng thái mới nhất theo LSN bất kể thứ tự nạp batch; còn overwrite-partition theo `event_date` giúp ghi đè các ngày nằm trong cửa sổ lookback một cách toàn vẹn và nhanh chóng mà không cần cập nhật từng dòng.
- Tombstone thay vì xoá hẳn hàng trong Silver: Giữ lại tombstone (`is_deleted=True`, xoá sạch PII) với `_lsn` lớn hơn giúp ngăn chặn việc các bản ghi cũ của ticket bị replay làm "hồi sinh" dữ liệu đã xoá, đồng thời phân biệt rõ giữa xóa logic và mất dữ liệu.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo tính bất biến (immutability) và khả năng tái lập (reproducibility) của tập huấn luyện mô hình, giúp kiểm thử và audit mô hình tại đúng thời điểm quá khứ mà không bị data leakage từ tương lai.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Bộ dữ liệu cỡ vừa (vài chục MB tới vài GB) nằm gọn trong bộ nhớ cục bộ, DuckDB/dbt chạy in-process cực nhanh, không tốn tài nguyên quản lý cluster JVM/worker phức tạp và chi phí vận hành như Spark.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   *Trả lời:* Trong thực tế production, quyền được xoá dữ liệu (GDPR / Right to be forgotten) luôn có ưu tiên pháp lý cao hơn tính bất biến của hệ thống lưu trữ. Để giải quyết mâu thuẫn này:
   - **Tách dữ liệu định danh (PII Vault / Crypto-shredding):** Lưu PII trong kho riêng với khoá mã hoá riêng cho từng người dùng (`user_id`). Khi có yêu cầu xoá, chỉ cần tiêu huỷ khoá mã hoá của người dùng đó (Crypto-shredding), toàn bộ văn bản trong các snapshot lịch sử sẽ lập tức trở thành chuỗi vô nghĩa không thể giải mã mà không cần sửa đổi cấu trúc file snapshot bất biến.
   - **Cơ chế Retrain / Re-snapshot có kiểm soát:** Với các snapshot dùng để train model thực tế, gắn danh sách Blacklist/Tombstone ID khi nạp vào DataLoader để loại bỏ dữ liệu của người đã rút phép; hoặc đánh phiên bản snapshot phụ (ví dụ: `v2026-08-12.1-redacted`) và ghi rõ lý do tuân thủ pháp lý vào audit log.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   *Trả lời:*
   - **Chốt PII ở đâu:** Nên đặt chốt phát hiện PII ngay tại ranh giới giữa Bronze và Silver (tại bước Ingestion/Staging sang Silver) để chặn PII không bao giờ lọt vào kho dữ liệu phân tích, và bổ sung một chốt thứ hai (second line of defense) trước khi dữ liệu vào Gold (chunking / training snapshot).
   - **Công nghệ áp dụng:** Do tên riêng tiếng Việt có ngữ cảnh đa dạng và không có cấu trúc cố định như email/SĐT, cần kết hợp: (1) Mô hình NER (Named Entity Recognition) chuyên dụng cho tiếng Việt (như PhoBERT-NER hoặc spaCy vi_core_news) để nhận diện thực thể `PER` (Person); (2) Bảng tra cứu đối chiếu (Lookup / Cross-reference) với danh sách người dùng (`silver_users.full_name`) để mask chính xác.
   - **Cách đo lường:** Định lượng bằng Precision, Recall và F1-score trên tập kiểm thử PII có gán nhãn thủ công (Ground truth test set); thiết lập SLA tự động (ví dụ: Recall $\ge 99.5\%$, Leak Rate = 0 trên sample audit ngẫu nhiên 1% dữ liệu hàng ngày trước khi release sang Gold).

## 5. Output (dán nguyên văn)

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ make test
..................................                                       [100%]
34 passed in 3.46s

$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make dbt
cd dbt_project && C:\Users\kienn\track2_labs\K4-Track02-Day17-NguyenTrungKien-2A202602764-DataPipelineEngineering\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
04:50:57  Running with dbt=1.12.5
04:50:58  Registered adapter: duckdb=1.11.0
04:50:58  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
04:50:58  
04:50:58  Concurrency: 1 threads (target='dev')
04:50:58  
04:50:59  1 of 19 START sql view model main.stg_events ................................... [RUN]
04:50:59  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.10s]
04:50:59  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
04:50:59  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.04s]
04:50:59  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
04:50:59  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.16s]
04:50:59  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
04:50:59  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.16s]
04:50:59  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
04:50:59  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.17s]
04:50:59  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
04:50:59  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.05s]
04:50:59  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
04:50:59  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.02s]
04:50:59  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
04:50:59  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.03s]
04:50:59  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
04:50:59  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.04s]
04:50:59  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
04:50:59  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.10s]
04:50:59  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
04:50:59  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.03s]
04:50:59  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
04:50:59  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
04:50:59  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
04:51:00  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.02s]
04:51:00  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
04:51:00  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.03s]
04:51:00  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
04:51:00  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.03s]
04:51:00  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
04:51:00  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
04:51:00  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.05s]
04:51:00  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
04:51:00  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.04s]
04:51:00  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
04:51:00  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.05s]
04:51:00  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
04:51:00  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.05s]
04:51:00  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
04:51:00  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.05s]
04:51:00  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
04:51:00  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.04s]
04:51:00  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
04:51:00  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.04s]
04:51:00  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.37s]
04:51:00  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
04:51:00  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.04s]
04:51:00  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
04:51:00  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
04:51:00  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
04:51:00  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]
04:51:00  
04:51:00  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.72 seconds (1.72s).
04:51:00  
04:51:00  Completed successfully
04:51:00  
04:51:00  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree

$ make bonus-llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```
