use axum::{routing::get, Router};
use socketioxide::{SocketIo, extract::SocketRef};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::info;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing::subscriber::set_global_default(FmtSubscriber::new())?;

    let (layer,io) = SocketIo::new_layer();

    io.ns("/", on_connect);

    

    let app = Router::new().route("/", get(|| async { "Hello world" }))
    
    .layer(
        ServiceBuilder::new()
        .layer(CorsLayer::permissive()).
        layer(layer),
    
    );

    info!("starting server...");

    let listener = TcpListener::bind("0.0.0.0:3000").await?;

    axum::serve(listener, app).await?;

    Ok(())
}

async fn on_connect (socket : SocketRef){

    info!("socket connected ! {:#?}" , socket.id);

}