use chrono::{DateTime, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{sqlite::SqliteConnectOptions, ConnectOptions, SqlitePool};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

// ==========================================
// ĐỊNH NGHĨA CÁC ĐỐI TƯỢNG DỮ LIỆU PHỨC TẠP
// ==========================================

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DynamicMetadata {
    device_type: String,
    ip_address: String,
    tags: Vec<String>,
    preferences: HashMap<String, String>,
    login_attempts: Vec<i64>,
}

#[derive(Debug, Clone)]
struct WideRowData {
    table_index: usize,
    c1: i64, c2: i64, c3: i64, c4: i64, c5: i64, c6: i64, c7: i64,
    c8: f64, c9: f64, c10: f64, c11: f64, c12: f64, c13: f64, c14: f64,
    c15: String, c16: String, c17: String, c18: String, c19: String, c20: String, c21: String,
    c22: bool, c23: bool, c24: bool, c25: bool, c26: bool, c27: bool, c28: bool,
}

#[derive(Debug, Clone)]
struct MixedRowData {
    table_index: usize,
    entity_id: String,
    hash_checksum: String,      // Chuỗi SHA-256 Hash
    payload_json: String,       // Siêu dữ liệu JSON động
    binary_blob: Vec<u8>,       // Dữ liệu nhị phân giả lập
    status_code: i32,
    updated_at: DateTime<Utc>,
}

enum DBWritePayload {
    Wide(WideRowData),
    Mixed(MixedRowData),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Tối ưu cấu hình SQLite mức tối đa
    let connection_options = SqliteConnectOptions::from_str("sqlite://extreme_test.db")?
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(10));

    let pool = SqlitePool::connect_with(connection_options).await?;

    println!("--- KHỞI TẠO CẤU TRÚC 12 BẢNG PHỨC TẠP ---");
    let mut tx = pool.begin().await?;

    // 1. Tạo 6 Bảng Rộng (Mỗi bảng 28 cột nguyên thủy)
    for i in 1..=6 {
        let query_str = format!(
            "CREATE TABLE IF NOT EXISTS table_wide_{} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                c1 INT, c2 INT, c3 INT, c4 INT, c5 INT, c6 INT, c7 INT,
                c8 REAL, c9 REAL, c10 REAL, c11 REAL, c12 REAL, c13 REAL, c14 REAL,
                c15 TEXT, c16 TEXT, c17 TEXT, c18 TEXT, c19 TEXT, c20 TEXT, c21 TEXT,
                c22 INT, c23 INT, c24 INT, c25 INT, c26 INT, c27 INT, c28 INT
            );", i
        );
        sqlx::query(&query_str).execute(&mut *tx).await?;
    }

    // 2. Tạo 6 Bảng Hỗn Hợp (Mã hóa Hash, JSON, BLOB)
    for i in 1..=6 {
        let query_str = format!(
            "CREATE TABLE IF NOT EXISTS table_mixed_{} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id TEXT NOT NULL,
                hash_checksum TEXT NOT NULL,
                payload_json TEXT,
                binary_blob BLOB,
                status_code INTEGER,
                updated_at TEXT
            );", i
        );
        sqlx::query(&query_str).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    println!("-> Khởi tạo Database thành công!");

    // Channel Buffer cực lớn để nhận dữ liệu từ các luồng tính toán song song
    let (tx_channel, mut rx_channel) = mpsc::channel::<DBWritePayload>(200_000);

    // ==================================================
    // LUỒNG WRITER TẬP TRUNG (Single Master Database Writer)
    // ==================================================
    let pool_clone = pool.clone();
    let writer_handle = tokio::spawn(async move {
        let mut batch = Vec::with_capacity(2000);
        let mut last_flush = Instant::now();
        let flush_interval = Duration::from_millis(15);

        while let Some(payload) = rx_channel.recv().await {
            batch.push(payload);

            if batch.len() >= 2000 || last_flush.elapsed() >= flush_interval {
                if !batch.is_empty() {
                    let mut db_tx = pool_clone.begin().await.unwrap();

                    for item in &batch {
                        match item {
                            DBWritePayload::Wide(w) => {
                                let query_str = format!(
                                    "INSERT INTO table_wide_{} (c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28)
                                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", w.table_index
                                );
                                sqlx::query(&query_str)
                                    .bind(w.c1).bind(w.c2).bind(w.c3).bind(w.c4).bind(w.c5).bind(w.c6).bind(w.c7)
                                    .bind(w.c8).bind(w.c9).bind(w.c10).bind(w.c11).bind(w.c12).bind(w.c13).bind(w.c14)
                                    .bind(&w.c15).bind(&w.c16).bind(&w.c17).bind(&w.c18).bind(&w.c19).bind(&w.c20).bind(&w.c21)
                                    .bind(w.c22).bind(w.c23).bind(w.c24).bind(w.c25).bind(w.c26).bind(w.c27).bind(w.c28)
                                    .execute(&mut *db_tx).await.unwrap();
                            }
                            DBWritePayload::Mixed(m) => {
                                let query_str = format!(
                                    "INSERT INTO table_mixed_{} (entity_id, hash_checksum, payload_json, binary_blob, status_code, updated_at)
                                     VALUES (?, ?, ?, ?, ?, ?)", m.table_index
                                );
                                sqlx::query(&query_str)
                                    .bind(&m.entity_id)
                                    .bind(&m.hash_checksum)
                                    .bind(&m.payload_json)
                                    .bind(&m.binary_blob)
                                    .bind(m.status_code)
                                    .bind(m.updated_at.to_rfc3339())
                                    .execute(&mut *db_tx).await.unwrap();
                            }
                        }
                    }
                    db_tx.commit().await.unwrap();
                    batch.clear();
                    last_flush = Instant::now();
                }
            }
        }

        // Xử lý nốt phần hàng đợi cuối cùng khi ngắt luồng
        if !batch.is_empty() {
            let mut db_tx = pool_clone.begin().await.unwrap();
            for item in batch {
                match item {
                    DBWritePayload::Wide(w) => {
                        let query_str = format!("INSERT INTO table_wide_{} (c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", w.table_index);
                        sqlx::query(&query_str).bind(w.c1).bind(w.c2).bind(w.c3).bind(w.c4).bind(w.c5).bind(w.c6).bind(w.c7).bind(w.c8).bind(w.c9).bind(w.c10).bind(w.c11).bind(w.c12).bind(w.c13).bind(w.c14).bind(&w.c15).bind(&w.c16).bind(&w.c17).bind(&w.c18).bind(&w.c19).bind(&w.c20).bind(&w.c21).bind(w.c22).bind(w.c23).bind(w.c24).bind(w.c25).bind(w.c26).bind(w.c27).bind(w.c28).execute(&mut *db_tx).await.unwrap();
                    }
                    DBWritePayload::Mixed(m) => {
                        let query_str = format!("INSERT INTO table_mixed_{} (entity_id, hash_checksum, payload_json, binary_blob, status_code, updated_at) VALUES (?, ?, ?, ?, ?, ?)", m.table_index);
                        sqlx::query(&query_str).bind(&m.entity_id).bind(&m.hash_checksum).bind(&m.payload_json).bind(&m.binary_blob).bind(m.status_code).bind(m.updated_at.to_rfc3339()).execute(&mut *db_tx).await.unwrap();
                    }
                }
            }
            db_tx.commit().await.unwrap();
        }
    });

    // ==================================================
    // SINH DỮ LIỆU ĐỒNG THỜI Ở MAIN THREAD (Tương thích Rust 2024)
    // ==================================================
    println!("--- ĐANG MÔ PHỎNG TÍNH TOÁN DỮ LIỆU PHỨC TẠP ---");
    let concurrency_tasks = 40;     // Luồng chạy song song gửi gói dữ liệu
    let items_per_task = 1500;       // Số record tạo ra trên mỗi luồng
    let total_records = concurrency_tasks * items_per_task * 2; // Gồm cả Wide và Mixed (120,000 Records)

    let start_time = Instant::now();
    let tx_arc = Arc::new(tx_channel);
    let mut client_tasks = vec![];

    let mut main_rng = rand::thread_rng();

    for task_id in 0..concurrency_tasks {
        let tx_clone = Arc::clone(&tx_arc);
        let mut local_payloads = Vec::with_capacity(items_per_task * 2);

        for step in 0..items_per_task {
            use rand::Rng; // Import trait Rng rõ ràng trong scope

            // Chọn ngẫu nhiên 1 trong 6 bảng cho mỗi loại
            let wide_table_idx = main_rng.gen_range(1..=6);
            let mixed_table_idx = main_rng.gen_range(1..=6);

            // A. TẠO DỮ LIỆU RỘNG 28 CỘT (Đã thay thế main_rng.gen())
            local_payloads.push(DBWritePayload::Wide(WideRowData {
                table_index: wide_table_idx,
                c1: main_rng.gen_range(1000..9999), c2: main_rng.gen_range(1000..9999), c3: main_rng.gen_range(1000..9999),
                c4: main_rng.gen_range(1000..9999), c5: main_rng.gen_range(1000..9999), c6: main_rng.gen_range(1000..9999), c7: main_rng.gen_range(1000..9999),
                c8: main_rng.gen_range(0.0..1.0), c9: main_rng.gen_range(0.0..1.0), c10: main_rng.gen_range(0.0..1.0),
                c11: main_rng.gen_range(0.0..1.0), c12: main_rng.gen_range(0.0..1.0), c13: main_rng.gen_range(0.0..1.0), c14: main_rng.gen_range(0.0..1.0),
                c15: format!("String_Field_A_{}_{}", task_id, step), c16: format!("String_Field_B_{}", step),
                c17: "Lorem ipsum dolor sit amet, consectetur adipiscing elit.".to_string(), c18: "Data_Audit_Logs".to_string(),
                c19: "System_Metrics_Value".to_string(), c20: "Production_Env".to_string(), c21: format!("UUID-{}-{}", task_id, step),
                c22: main_rng.gen_bool(0.5), c23: main_rng.gen_bool(0.5), c24: main_rng.gen_bool(0.5),
                c25: main_rng.gen_bool(0.5), c26: main_rng.gen_bool(0.5), c27: main_rng.gen_bool(0.5), c28: main_rng.gen_bool(0.5),
            }));

            // B. TẠO DỮ LIỆU ĐỘNG (HASH PHỨC TẠP & JSONB GIẢ LẬP)
            let mut hasher = Sha256::new();
            let raw_info = format!("payload-data-{}-{}-{}", task_id, step, mixed_table_idx);
            hasher.update(raw_info.as_bytes());
            let hash_result = hex::encode(hasher.finalize());

            let mut prefs = HashMap::new();
            prefs.insert("theme".to_string(), "dark_mode".to_string());
            prefs.insert("language".to_string(), "vi-VN".to_string());
            prefs.insert("cache_strategy".to_string(), "lru_redis".to_string());

            let meta = DynamicMetadata {
                device_type: if step % 2 == 0 { "Mobile/iOS".to_string() } else { "Desktop/Linux".to_string() },
                ip_address: format!("192.168.{}.{}", main_rng.gen_range(1..254), main_rng.gen_range(1..254)),
                tags: vec!["production".to_string(), "heavy_load".to_string(), "encrypted".to_string()],
                preferences: prefs,
                login_attempts: vec![1024, 2048, 4096, main_rng.gen_range(5000..9000)],
            };
            let json_str = serde_json::to_string(&meta).unwrap();

            // Thay thế main_rng.gen::<u8>() bằng gen_range để tránh từ khóa gen hệ thống
            let mut fake_binary = Vec::with_capacity(32);
            for _ in 0..32 {
                fake_binary.push(main_rng.gen_range(0..=255) as u8);
            }

            local_payloads.push(DBWritePayload::Mixed(MixedRowData {
                table_index: mixed_table_idx,
                entity_id: format!("ENT-{}", hash_result[0..8].to_string()),
                hash_checksum: hash_result,
                payload_json: json_str,
                binary_blob: fake_binary,
                status_code: main_rng.gen_range(200..505),
                updated_at: Utc::now(),
            }));
        }

        let handle = tokio::spawn(async move {
            for payload in local_payloads {
                if tx_clone.send(payload).await.is_err() {
                    break;
                }
            }
        });
        client_tasks.push(handle);
    }
    // Đợi các luồng đẩy dữ liệu vào channel xong
    for handle in client_tasks {
        handle.await?;
    }
    drop(tx_arc); // Giải phóng channel chính để Writer biết dữ liệu đã gửi xong
    // Đợi luồng ghi ghi toàn bộ xuống SQLite
    writer_handle.await?;
    let duration = start_time.elapsed();
    // ==========================================
    // KIỂM TRA TỔNG HỢP KẾT QUẢ TRÊN 12 BẢNG
    // ==========================================
    println!("--------------------------------------------------");
    println!("--- THỐNG KÊ KẾT QUẢ TRÊN 12 BẢNG HỆ THỐNG ---");
    let mut total_inserted_db = 0;
    for i in 1..=6 {
        let count_wide: (i64,) = sqlx::query_as(&format!("SELECT COUNT() FROM table_wide_{}", i))
            .fetch_one(&pool).await?;
        let count_mixed: (i64,) = sqlx::query_as(&format!("SELECT COUNT() FROM table_mixed_{}", i))
            .fetch_one(&pool).await?;
        println!("Bảng wide_{}: {} records  ||  Bảng mixed_{}: {} records", i, count_wide.0, i, count_mixed.0);
        total_inserted_db += count_wide.0 + count_mixed.0;
    }
    println!("--------------------------------------------------");
    println!("Tổng thời gian xử lý cấu trúc nặng: {:?}", duration);
    println!("Tổng số bản ghi thực tế trong SQLite: {}", total_inserted_db);
    println!("Tốc độ Ghi Hỗn Hợp Hệ Thống: {:.2} writes/second", (total_records as f64) / duration.as_secs_f64());
    println!("--------------------------------------------------");
    pool.close().await;
    Ok(())
}