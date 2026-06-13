use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn tokio_tasks_mpsc_example() {
    let (tx, mut rx) = mpsc::channel::<String>(10);

    // Producer 1
    let tx1 = tx.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(1)).await;

        tx1.send("Job from producer 1".to_string())
            .await
            .unwrap();
    });

    // Producer 2
    let tx2 = tx.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(2)).await;

        tx2.send("Job from producer 2".to_string())
            .await
            .unwrap();
    });

    // Producer 3
    let tx3 = tx.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(3)).await;

        tx3.send("Job from producer 3".to_string())
            .await
            .unwrap();
    });

    // Drop original sender
    drop(tx);

    // Consumera
    while let Some(msg) = rx.recv().await {
        println!("Received: {}", msg);
    }

    println!("Channel closed");
}