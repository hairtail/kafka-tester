use std::{env, sync::Arc, time::Duration};

use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct KafkaSolutionInfo {
    coin_type: String,
    pool_id: String,
    user_address: String,
    worker_id: String,
    height: u64,
    block_hash: String,
    difficulty: u64,
    status: String,
    timestamp: i64,
    is_email_user: bool,
    address_type: u8,
}

impl Default for KafkaSolutionInfo {
    fn default() -> Self {
        Self {
            coin_type: "TSSC".to_string(),
            pool_id: "test_pool".to_string(),
            user_address: "tssctestaddress".to_string(),
            worker_id: "worker".to_string(),
            height: 1,
            block_hash: "test_block".to_string(),
            difficulty: 100,
            status: "TEST".to_string(),
            timestamp: 1000,
            is_email_user: false,
            address_type: 1,
        }
    }
}

#[tokio::main]
async fn main() {
    let kafka_bootstrap_servers =
        env::var("KAFKA_BOOTSTRAP_SERVERS").expect("KAFKA_BOOTSTRAP_SERVERS undefined");
    let kafka_producer: Arc<FutureProducer> = Arc::new(
        ClientConfig::new()
            .set("bootstrap.servers", kafka_bootstrap_servers.to_string())
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanism", "PLAIN")
            .set("sasl.username", "")
            .set("sasl.password", "")
            .create()
            .expect("kafka producer creation error"),
    );
    let mut counter = 1000000;
    loop {
        if counter <= 0 {
            break;
        }
        let producer = kafka_producer.clone();
        tokio::spawn(async move {
            let solution = KafkaSolutionInfo::default();
            if let Err(error) = producer
                .send(
                    FutureRecord::<String, String>::to("subspace_solution_topic")
                        .payload(&serde_json::to_string(&vec![solution]).unwrap()),
                    Duration::from_secs(3),
                )
                .await
            {
                eprintln!("kafka producer {:?}", error);
            }
        });
        counter -= 1;
    }
}
