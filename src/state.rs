use std::collections::{HashMap, VecDeque};

use serde::Serialize;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize)]
pub struct Message {
    pub text: String,
    pub user: String,
    pub date: chrono::DateTime<chrono::Utc>,
}

pub type RoomStore = HashMap<String, VecDeque<Message>>;

#[derive(Default)]
pub struct MessageStore {
    messages: RwLock<RoomStore>,
}

impl MessageStore {
    pub async fn insert(&self, room: &str, message: Message) {
        let mut rooms = self.messages.write().await;

        let messages = rooms.entry(room.to_owned()).or_default();

        messages.push_front(message);
        messages.truncate(20);
    }

    pub async fn get(&self, room: &str) -> Vec<Message> {
        self.messages
            .read()
            .await
            .get(room)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .rev()
            .collect()
    }
}