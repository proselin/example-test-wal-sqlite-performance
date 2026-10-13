use chrono::{DateTime, Utc};
use sqlx::{sqlite::SqliteConnectOptions, SqlitePool};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

// 1. Định nghĩa cấu trúc dữ liệu cần ghi vào DB
#[derive(Debug, Clone)]
struct LogMessage {
    user_id: i32,
    action: String,
    created_at: DateTime<Utc>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 2. Cấu hình SQLite tối ưu (Bật WAL mode, giảm synchronous để tối đa tốc độ ghi)
    let connection_options = SqliteConnectOptions::from_str("sqlite://test_speed.db")?
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(5));

    let pool = SqlitePool::connect_with(connection_options).await?;

    // Tạo bảng test nếu chưa có
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS user_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            action TEXT,
            created_at TEXT
        );",
    )
        .execute(&pool)
        .await?;

    println!("--- BẮT ĐẦU KIỂM TRA HIỆU NĂNG ---");

    // 3. Tạo một MPSC Channel để nhận các yêu cầu ghi từ nhiều luồng (Concurrencies)
    // Buffer size = 50,000 để chứa tạm dữ liệu trong bộ nhớ khi các luồng đổ về quá nhanh
    let (tx, mut rx) = mpsc::channel::<LogMessage>(50_000);

    // 4. LUỒNG WRITER DUY NHẤT (Background Worker)
    // Luồng này đứng đợi channel, gom dữ liệu thành các Batch và ghi xuống SQLite
    let pool_clone = pool.clone();
    let writer_handle = tokio::spawn(async move {
        let mut batch = Vec::with_capacity(1000);
        let mut last_flush = Instant::now();
        let flush_interval = Duration::from_millis(10); // Cứ sau 10ms hoặc đủ 1000 records thì ghi 1 lần

        while let Some(msg) = rx.recv().await {
            batch.push(msg);

            // Nếu đủ 1000 records hoặc đã quá 10ms kể từ lần ghi trước, tiến hành Ghi Hàng Loạt
            if batch.len() >= 1000 || last_flush.elapsed() >= flush_interval {
                if !batch.is_empty() {
                    // Thực hiện ghi toàn bộ batch vào 1 Transaction duy nhất
                    let mut tx = pool_clone.begin().await.unwrap();

                    for item in &batch {
                        sqlx::query("INSERT INTO user_logs (user_id, action, created_at) VALUES (?, ?, ?)")
                            .bind(item.user_id)
                            .bind(&item.action)
                            .bind(item.created_at.to_rfc3339())
                            .execute(&mut *tx)
                            .await
                            .unwrap();
                    }

                    tx.commit().await.unwrap(); // Commit 1 lần duy nhất cho cả 1000 bản ghi
                    batch.clear();
                    last_flush = Instant::now();
                }
            }
        }

        // Xử lý nốt dữ liệu còn sót lại trong batch khi channel bị đóng
        if !batch.is_empty() {
            let mut tx = pool_clone.begin().await.unwrap();
            for item in &batch {
                sqlx::query("INSERT INTO user_logs (user_id, action, created_at) VALUES (?, ?, ?)")
                    .bind(item.user_id)
                    .bind(&item.action)
                    .bind(item.created_at.to_rfc3339())
                    .execute(&mut *tx)
                    .await
                    .unwrap();
            }
            tx.commit().await.unwrap();
        }
    });

    // 5. MÔ PHỎNG HÀNG LOẠT LUỒNG GHI ĐỒNG THỜI (High Concurrency)
    let total_tasks = 50;
    let records_per_task = 2_000;
    let total_records = total_tasks * records_per_task;

    let start_time = Instant::now();
    let tx_arc = Arc::new(tx);
    let mut client_tasks = vec![];

    // Tạo sẵn trình tạo số ngẫu nhiên ở luồng chính (Main Thread)
    let mut rng = rand::thread_rng();

    for task_id in 0..total_tasks {
        let tx_clone = Arc::clone(&tx_arc);

        // BƯỚC 1: Tạo sẵn dữ liệu ngẫu nhiên ở luồng chính, KHÔNG để trong async block
        let mut local_messages = Vec::with_capacity(records_per_task);
        for _ in 0..records_per_task {
            use rand::Rng; // Đảm bảo đã import trait Rng
            local_messages.push(LogMessage {
                user_id: rng.gen_range(1..1000),
                action: format!("User {} performed action {}", task_id, rng.gen_range(1..5)),
                created_at: Utc::now(),
            });
        }

        // BƯỚC 2: Đẩy mảng dữ liệu đã tạo sẵn vào luồng Async (Lúc này không còn dính dáng gì tới biến `rng` nữa)
        let handle = tokio::spawn(async move {
            for msg in local_messages {
                if tx_clone.send(msg).await.is_err() {
                    break;
                }
            }
        });
        client_tasks.push(handle);
    }

    // Đợi cho tất cả các luồng mô phỏng gửi dữ liệu xong
    for handle in client_tasks {
        handle.await?;
    }

    // Đóng channel gửi để thông báo cho luồng Writer biết dữ liệu đã hết và có thể kết thúc
    drop(tx_arc);

    // Đợi luồng Writer xử lý và ghi xong toàn bộ dữ liệu xuống đĩa cứng
    writer_handle.await?;

    let duration = start_time.elapsed();

    // 6. ĐẾM VÀ IN KẾT QUẢ THỰC TẾ
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM user_logs")
        .fetch_one(&pool)
        .await?;

    println!("--------------------------------------");
    println!("Thời gian chạy: {:?}", duration);
    println!("Tổng số bản ghi đã ghi thành công: {}", count.0);
    println!(
        "Tốc độ trung bình: {:.2} writes/second",
        (total_records as f64) / duration.as_secs_f64()
    );
    println!("--------------------------------------");

    pool.close().await;
    Ok(())
}