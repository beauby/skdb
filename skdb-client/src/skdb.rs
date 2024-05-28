use crate::mux::*;
use crate::orchestration::*;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite; //
use uuid::Uuid;

pub struct SkdbClient {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    stream_id: u32,
}

impl SkdbClient {
    pub async fn connect(
        url: &str,
        access_key: &str,
        private_key: &str,
        device_uuid: &str,
    ) -> Result<Self, tokio_tungstenite::tungstenite::Error> {
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await?;

        ws.send(tokio_tungstenite::tungstenite::protocol::Message::binary(
            AuthMsg::new(
                private_key,
                access_key,
                Uuid::parse_str(device_uuid).expect("Invalid device_uuid"),
            ),
        ))
        .await?;

        Ok(Self { ws, stream_id: 1 })
    }

    async fn send_message(
        &mut self,
        msg: MuxMsg,
    ) -> Result<(), tokio_tungstenite::tungstenite::Error> {
        self.ws
            .send(tokio_tungstenite::tungstenite::protocol::Message::binary(
                msg,
            ))
            .await
    }

    pub async fn exec(_sql: String) {
        todo!()
    }

    pub async fn watch(_sql: String, _params: String, _cb: fn() -> ()) {
        todo!()
    }

    pub async fn mirror(_table: String, _schema: String, _filter_expr: String) {
        todo!()
    }
}
