use futures_util::{SinkExt, StreamExt};
use std::str;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use uuid::Uuid;

use skdb_client::mux::{AuthMsg, CloseMsg, DataMsg, GoAwayMsg, MuxMsg, MuxMsgPayload, PingMsg};
use skdb_client::orchestration::{Data, RequestTail, RequestTailBatch, Schema, SchemaScope};

use skdb_client::SkdbClient;

#[tokio::main]
async fn main() {
    // let url = "wss://api.skiplabs.io/dbs/lucasdb/connection";
    let url = "ws://localhost:8110/dbs/lucasdb/connection";
    let access_key = "root";
    let private_key = "DjbdTJXJm3cl352FfOG6wHw8ZmU700NMbbYf/YzGVkY="; // "h0kTjaNUhHG0GjfLgjLhrkipdXFVVvYPMulR7ZqM5os=";
    let device_uuid = Uuid::new_v4().hyphenated().to_string();
    // let mut skdb_client = SkdbClient::connect(url, access_key, private_key, device_uuid.as_str())
    //     .await
    //     .expect("Failed to connect");

    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    // println!("WebSocket handshake has been successfully completed");

    let (mut write, read) = ws_stream.split();

    write
        .send(Message::binary(PingMsg {}.to_mux_msg(0)))
        .await
        .unwrap();

    write
        .send(Message::binary(
            AuthMsg::new(
                private_key,
                "root",
                Uuid::parse_str(&device_uuid).expect(""),
            )
            .to_mux_msg(0),
        ))
        .await
        .unwrap();

    write
        .send(Message::binary(PingMsg {}.to_mux_msg(0)))
        .await
        .unwrap();

    // write
    //     .send(Message::binary(ErrorMsg::new(0, 0, "").to_mux_msg(0)))
    //     .await
    //     .unwrap();

    // write
    // .send(Message::binary(PingMsg {}.to_mux_msg(0)))
    // .await
    // .unwrap();

    write
        .send(Message::binary(
            Schema::new(SchemaScope::All, "", "").to_mux_msg(1),
        ))
        .await
        .unwrap();

    write
        .send(Message::binary(
            RequestTailBatch {
                requests: vec![RequestTail::new(
                    0,
                    "skdb_users",
                    "(userID TEXT PRIMARY KEY, privateKey TEXT NOT NULL)",
                    "",
                    "{}",
                )],
            }
            .to_mux_msg(5),
        ))
        .await
        .unwrap();

    let read_future = read.for_each(|message| async {
        println!("receiving...");
        let data = message.unwrap().into_data();
        let msg: MuxMsg = data.as_slice().into();
        println!("{:?}", msg);
        match msg.payload {
            MuxMsgPayload::Data(DataMsg::Data(Data { fin, payload })) => {
                println!("{}", str::from_utf8(payload.as_slice()).unwrap())
            }
            _ => (),
        }
        // // tokio::io::stdout().write(format!("{:x?}", &data).into()).await.unwrap();
        println!("");
        println!("received.");
    });
    read_future.await;
}
