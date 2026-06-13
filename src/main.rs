use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{broadcast, RwLock};

#[derive(Clone)]
struct AppState {
    rooms: Arc<RwLock<HashMap<String, broadcast::Sender<String>>>>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ClientMessage {
    #[serde(rename = "join")]
    Join { room: String },

    #[serde(rename = "message")]
    Message { room: String, text: String },
}

#[derive(Debug, Serialize)]
struct ServerMessage {
    room: String,
    text: String,
}

#[tokio::main]
async fn main() {
    let state = Arc::new(AppState {
        rooms: Arc::new(RwLock::new(HashMap::new())),
    });

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .unwrap();

    println!("Server running on 0.0.0.0:3000");

    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn get_room_sender(
    state: &AppState,
    room: &str,
) -> broadcast::Sender<String> {
    let mut rooms = state.rooms.write().await;

    rooms
        .entry(room.to_string())
        .or_insert_with(|| {
            let (tx, _) = broadcast::channel(100);
            tx
        })
        .clone()
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    println!("Client connected");

    let (mut sender, mut receiver) = socket.split();

    let mut room_rx: Option<broadcast::Receiver<String>> = None;
    let mut current_room: Option<String> = None;

    loop {
        tokio::select! {

            msg = receiver.next() => {
                let Some(Ok(msg)) = msg else {
                    break;
                };

                match msg {
                    Message::Text(text) => {

                        let parsed: ClientMessage = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };

                        match parsed {

                            ClientMessage::Join { room } => {
                                println!("Joining room: {}", room);

                                let tx = get_room_sender(&state, &room).await;

                                room_rx = Some(tx.subscribe());
                                current_room = Some(room);
                            }

                            ClientMessage::Message { room, text } => {
                                let tx = get_room_sender(&state, &room).await;

                                let payload = serde_json::to_string(
                                    &ServerMessage {
                                        room: room.clone(),
                                        text,
                                    }
                                ).unwrap();

                                let _ = tx.send(payload);
                            }
                        }
                    }

                    Message::Close(_) => {
                        break;
                    }

                    _ => {}
                }
            }

            broadcast_msg = async {
                match &mut room_rx {
                    Some(rx) => rx.recv().await.ok(),
                    None => None,
                }
            }, if room_rx.is_some() => {

                if let Some(msg) = broadcast_msg {

                    if sender.send(Message::Text(msg.into())).await.is_err() {
                        break;
                    }
                }
            }
        }
    }

    println!("Client disconnected from {:?}", current_room);
}