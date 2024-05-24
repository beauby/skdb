use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use base64::prelude::*;
use uuid::Uuid;
use rand::RngCore;
use chrono::Utc;
use ring;
use std::fmt;

enum MuxMsg {
    Ping(PingMsg),
    Pong(PongMsg),
    Auth(AuthMsg),
    Data(DataMsg),
}

impl From<Vec<u8>> for MuxMsg {
    fn from(data: Vec<u8>) -> MuxMsg {
        match data[0] {
            0x0 => MuxMsg::Auth(data.into()),
            0x2 => MuxMsg::Data(data.into()),
            0x5 => MuxMsg::Ping(data.into()),
            0x6 => MuxMsg::Pong(data.into()),
            _ => panic!("Unknown message type {}", data[0])
        }
    }
}

impl fmt::Display for MuxMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MuxMsg::Ping(msg) => msg.fmt(f),
            MuxMsg::Pong(msg) => msg.fmt(f),
            MuxMsg::Auth(msg) => msg.fmt(f),
            MuxMsg::Data(msg) => msg.fmt(f),
        }
    }
}

struct PingMsg {}

impl From<PingMsg> for Vec<u8> {
    fn from(_msg: PingMsg) -> Self {
        let mut buf = vec![0u8; 4];
        buf[0] = 0x5;

        buf
    }
}

impl From<Vec<u8>> for PingMsg {
    fn from(_data: Vec<u8>) -> Self {
        PingMsg{}
    }
}

impl fmt::Display for PingMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ping")
    }
}


struct PongMsg {}

impl From<PongMsg> for Vec<u8> {
    fn from(_msg: PongMsg) -> Self {
        let mut buf = vec![0u8; 4];
        buf[0] = 0x6;

        buf
    }
}

impl From<Vec<u8>> for PongMsg {
    fn from(_data: Vec<u8>) -> Self {
        PongMsg{}
    }
}

impl fmt::Display for PongMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pong")
    }
}


struct AuthMsg {
    access_key: Vec<u8>,
    nonce: [u8; 8],
    signature: [u8; 32],
    device_uuid: [u8; 36],
    date: String,
    client_version: String,
}

impl AuthMsg {
    pub fn new(private_key: String, access_key: String, device_uuid: [u8; 36]) -> Self {
        let mut nonce = [0u8; 8];
        rand::thread_rng().fill_bytes(&mut nonce);
        let date = Utc::now().format("%FT%T%.3fZ").to_string();
        let content = format!("auth{}{}{}", access_key, date, BASE64_STANDARD.encode(nonce));
        let signature: [u8; 32] = ring::hmac::sign(
            &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &BASE64_STANDARD.decode(private_key).unwrap()),
            content.as_bytes()
        ).as_ref().try_into().unwrap();
    
        AuthMsg{
            access_key: access_key.into(),
            nonce: nonce,
            signature: signature,
            device_uuid: device_uuid,
            date: date,
            client_version: "js-0.0.67".to_string(), // FIXME
        }
    }
}

impl From<AuthMsg> for Vec<u8> {
    fn from(msg: AuthMsg) -> Self { 
        let mut buf = vec![0u8; 137 + msg.client_version.len()];

        buf[8..(8 + msg.access_key.len())].copy_from_slice(msg.access_key.as_slice());
        buf[35..43].copy_from_slice(&msg.nonce);
        buf[43..75].copy_from_slice(&msg.signature);
        buf[75..111].copy_from_slice(&msg.device_uuid);
        buf[111] = 0x0; // 24-byte date
        buf[112..136].copy_from_slice(msg.date.as_bytes());
        if msg.client_version.len() > 255 {
            panic!("Cannot encode client version, too long")
        }
        buf[136] = u8::try_from(msg.client_version.len()).unwrap();
        buf[137..(137 + msg.client_version.len())].copy_from_slice(msg.client_version.as_bytes());

        buf
    }
}

impl From<Vec<u8>> for AuthMsg {
    fn from(_data: Vec<u8>) -> Self {
        todo!()
    }
}


impl fmt::Display for AuthMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "auth")
    }
}


struct DataMsg {
    payload: Vec<u8>,
}

impl DataMsg {
    pub fn new(payload: Vec<u8>) -> Self {
        DataMsg{
            payload: payload,
        }
    }
}

impl From<DataMsg> for Vec<u8> {
    fn from(msg: AuthMsg) -> Self { 
        let mut buf = vec![0u8; 1 + msg.payload.len()];

        buf[8..(8 + msg.access_key.len())].copy_from_slice(msg.access_key.as_slice());
        buf[35..43].copy_from_slice(&msg.nonce);
        buf[43..75].copy_from_slice(&msg.signature);
        buf[75..111].copy_from_slice(&msg.device_uuid);
        buf[111] = 0x0; // 24-byte date
        buf[112..136].copy_from_slice(msg.date.as_bytes());
        if msg.client_version.len() > 255 {
            panic!("Cannot encode client version, too long")
        }
        buf[136] = u8::try_from(msg.client_version.len()).unwrap();
        buf[137..(137 + msg.client_version.len())].copy_from_slice(msg.client_version.as_bytes());

        buf
    }
}

impl From<Vec<u8>> for AuthMsg {
    fn from(_data: Vec<u8>) -> Self {
        todo!()
    }
}


impl fmt::Display for AuthMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "auth")
    }
}



#[tokio::main]
async fn main() {
    let url = url::Url::parse("wss://api.skiplabs.io/dbs/lucasdb/connection").unwrap();

    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    // println!("WebSocket handshake has been successfully completed");

    let (mut write, read) = ws_stream.split();
    
    let mut uuid = [0u8; 36];
    Uuid::new_v4().hyphenated().encode_lower(&mut uuid);
    let msg = AuthMsg::new("h0kTjaNUhHG0GjfLgjLhrkipdXFVVvYPMulR7ZqM5os=".to_string(), "root".to_string(), uuid);
    write.send(Message::binary(msg)).await.unwrap();
    write.send(Message::binary(PingMsg{})).await.unwrap();

    let read_future = read.for_each(|message| async {
        println!("receiving...");
        let data = message.unwrap().into_data();
        let msg: MuxMsg = data.into();
        println!("{}", msg);
        // tokio::io::stdout().write(format!("{:x?}", &data).into()).await.unwrap();
        println!("");
        println!("received.");
    });
    read_future.await;
}
