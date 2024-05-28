use crate::orchestration::*;
use base64::prelude::*;
use chrono::{DateTime, SecondsFormat, Utc};
use rand::RngCore;
use std::str;
use uuid::Uuid;

#[derive(Debug)]
pub struct MuxMsg {
    pub stream: u32,
    pub payload: MuxMsgPayload,
}

impl MuxMsg {
    fn kind(&self) -> u8 {
        match self.payload {
            MuxMsgPayload::Auth(_) => 0x0,
            MuxMsgPayload::GoAway(_) => 0x1,
            MuxMsgPayload::Data(_) => 0x2,
            MuxMsgPayload::Close(_) => 0x3,
            MuxMsgPayload::Reset(_) => 0x4,
            MuxMsgPayload::Ping(_) => 0x5,
            MuxMsgPayload::Pong(_) => 0x6,
        }
    }
}

impl From<&[u8]> for MuxMsg {
    fn from(data: &[u8]) -> Self {
        let type_and_stream = u32::from_be_bytes(data[0..4].try_into().unwrap());
        let data = &data[4..];
        let stream = type_and_stream & 0xffffff;
        let typ = type_and_stream >> 24;

        Self {
            stream,
            payload: match typ {
                0x0 => MuxMsgPayload::Auth(data.into()),
                0x1 => MuxMsgPayload::GoAway(data.into()),
                0x2 => MuxMsgPayload::Data(data.into()),
                0x3 => MuxMsgPayload::Close(data.into()),
                0x4 => MuxMsgPayload::Reset(data.into()),
                0x5 => MuxMsgPayload::Ping(data.into()),
                0x6 => MuxMsgPayload::Pong(data.into()),
                t => panic!("Unknown message type {} ({:?})", t, data),
            },
        }
    }
}

impl From<MuxMsg> for Vec<u8> {
    fn from(msg: MuxMsg) -> Self {
        let mut buf = vec![];
        buf.push(msg.kind());
        buf.extend(&msg.stream.to_be_bytes()[1..4]);
        buf.extend(Vec::<u8>::from(msg.payload));

        println!("Sending: {:x?}", buf);
        println!("{:?}", MuxMsg::from(buf.as_slice()));
        buf
    }
}

#[derive(Debug)]
pub enum MuxMsgPayload {
    Auth(AuthMsg),
    GoAway(GoAwayMsg),
    Data(DataMsg),
    Close(CloseMsg),
    Reset(ResetMsg),
    Ping(PingMsg),
    Pong(PongMsg),
}

impl From<MuxMsgPayload> for Vec<u8> {
    fn from(payload: MuxMsgPayload) -> Self {
        match payload {
            MuxMsgPayload::Auth(msg) => msg.into(),
            MuxMsgPayload::GoAway(msg) => msg.into(),
            MuxMsgPayload::Data(msg) => msg.into(),
            MuxMsgPayload::Close(msg) => msg.into(),
            MuxMsgPayload::Reset(msg) => msg.into(),
            MuxMsgPayload::Ping(msg) => msg.into(),
            MuxMsgPayload::Pong(msg) => msg.into(),
        }
    }
}

#[derive(Debug)]
pub struct AuthMsg {
    access_key: String,
    nonce: [u8; 8],
    signature: [u8; 32],
    device_uuid: Uuid,
    date: DateTime<Utc>,
    client_version: String,
}

impl AuthMsg {
    pub fn to_mux_msg(self, stream: u32) -> MuxMsg {
        MuxMsg {
            stream,
            payload: MuxMsgPayload::Auth(self),
        }
    }

    pub fn new(private_key: &str, access_key: &str, device_uuid: Uuid) -> Self {
        let mut nonce = [0u8; 8];
        rand::thread_rng().fill_bytes(&mut nonce);
        let date = Utc::now();
        let content = format!(
            "auth{}{}{}",
            access_key,
            date.to_rfc3339_opts(SecondsFormat::Millis, true),
            BASE64_STANDARD.encode(nonce)
        );
        let signature: [u8; 32] = ring::hmac::sign(
            &ring::hmac::Key::new(
                ring::hmac::HMAC_SHA256,
                &BASE64_STANDARD.decode(private_key).unwrap(),
            ),
            content.as_bytes(),
        )
        .as_ref()
        .try_into()
        .unwrap();

        let client_version = "js-0.0.67".to_string(); // FIXME

        Self {
            access_key: access_key.to_string(),
            nonce,
            signature,
            device_uuid,
            date,
            client_version,
        }
    }
}

impl From<&[u8]> for AuthMsg {
    fn from(data: &[u8]) -> Self {
        let data = &data[4..];

        let access_key_len = data[..27].iter().position(|&c| c == 0).unwrap_or(27);
        let access_key = str::from_utf8(&data[..access_key_len]).unwrap().to_string();
        let data = &data[27..];

        let nonce = data[..8].try_into().unwrap();
        let data = &data[8..];

        let signature = data[..32].try_into().unwrap();
        let data = &data[32..];

        let device_uuid = Uuid::parse_str(str::from_utf8(&data[..36]).unwrap()).unwrap();
        let data = &data[36..];

        let date_size = if data[0] == 0 { 24 } else { 27 };
        let data = &data[1..];

        let date: DateTime<Utc> =
            DateTime::parse_from_rfc3339(str::from_utf8(&data[..date_size]).unwrap())
                .unwrap()
                .into();
        let data = &data[date_size..];

        let client_version_len = data[0] as usize;
        let data = &data[1..];

        let client_version = str::from_utf8(&data[..client_version_len])
            .unwrap()
            .to_string();

        Self {
            access_key,
            nonce,
            signature,
            device_uuid,
            date,
            client_version,
        }
    }
}

impl From<AuthMsg> for Vec<u8> {
    fn from(msg: AuthMsg) -> Self {
        let mut buf = vec![];
        buf.extend([0u8; 4]);
        buf.extend(format!("{:\0<27}", msg.access_key).as_bytes());
        buf.extend(&msg.nonce);
        buf.extend(&msg.signature);
        buf.extend(msg.device_uuid.as_hyphenated().to_string().as_bytes());
        buf.push(0x0); // 24-byte date
        buf.extend(
            msg.date
                .to_rfc3339_opts(SecondsFormat::Millis, true)
                .as_bytes(),
        );
        if msg.client_version.len() > 255 {
            panic!("Cannot encode client version, too long")
        }
        buf.push(u8::try_from(msg.client_version.len()).unwrap());
        buf.extend(msg.client_version.as_bytes());

        buf
    }
}

#[derive(Debug)]
pub struct GoAwayMsg {
    last_stream: u32,
    code: u32,
    msg: String,
}

impl GoAwayMsg {
    pub fn new(last_stream: u32, code: u32, msg: &str) -> Self {
        Self {
            last_stream,
            code,
            msg: msg.to_string(),
        }
    }
    pub fn to_mux_msg(self, stream: u32) -> MuxMsg {
        MuxMsg {
            stream,
            payload: MuxMsgPayload::GoAway(self),
        }
    }
}

impl From<&[u8]> for GoAwayMsg {
    fn from(data: &[u8]) -> Self {
        let last_stream = u32::from_be_bytes(data[..4].try_into().unwrap()) & 0xffffff;
        let data = &data[4..];

        let code = u32::from_be_bytes(data[..4].try_into().unwrap());
        let data = &data[4..];

        let msg_len = u32::from_be_bytes(data[..4].try_into().unwrap()) as usize;
        let data = &data[4..];

        let msg = str::from_utf8(&data[..msg_len]).unwrap().to_string();

        Self {
            last_stream,
            code,
            msg,
        }
    }
}

impl From<GoAwayMsg> for Vec<u8> {
    fn from(error: GoAwayMsg) -> Self {
        let mut buf = vec![];
        buf.extend(error.last_stream.to_be_bytes());
        buf.extend(error.code.to_be_bytes());
        buf.extend((error.msg.len() as u32).to_be_bytes());
        buf.extend(error.msg.as_bytes());

        buf
    }
}

#[derive(Debug)]
pub enum DataMsg {
    Data(Data),
    Query(Query),
    RequestTail(RequestTail),
    PushPromise(PushPromise),
    Schema(Schema),
    CreateDb(CreateDb),
    CreateUser(CreateUser),
    RequestTailBatch(RequestTailBatch),
    CredentialsResponse(CredentialsResponse),
}

impl DataMsg {
    pub fn to_mux_msg(self, stream: u32) -> MuxMsg {
        MuxMsg {
            stream,
            payload: MuxMsgPayload::Data(self),
        }
    }

    pub fn kind(&self) -> u8 {
        match self {
            DataMsg::Data(_) => 0x0,
            DataMsg::Query(_) => 0x1,
            DataMsg::RequestTail(_) => 0x2,
            DataMsg::PushPromise(_) => 0x3,
            DataMsg::Schema(_) => 0x4,
            DataMsg::CreateDb(_) => 0x5,
            DataMsg::CreateUser(_) => 0x6,
            DataMsg::RequestTailBatch(_) => 0x7,
            DataMsg::CredentialsResponse(_) => 0x80,
        }
    }
}

impl From<&[u8]> for DataMsg {
    fn from(data: &[u8]) -> Self {
        let typ = data[0];
        let data = &data[1..];
        match typ {
            0x0 => DataMsg::Data(data.into()),
            0x1 => DataMsg::Query(data.into()),
            0x2 => DataMsg::RequestTail(data.into()),
            0x3 => DataMsg::PushPromise(data.into()),
            0x4 => DataMsg::Schema(data.into()),
            0x5 => DataMsg::CreateDb(data.into()),
            0x6 => DataMsg::CreateUser(data.into()),
            0x7 => DataMsg::RequestTailBatch(data.into()),
            0x80 => DataMsg::CredentialsResponse(data.into()),
            t => panic!("Unknown data message type {}", t),
        }
    }
}

impl From<DataMsg> for Vec<u8> {
    fn from(msg: DataMsg) -> Self {
        let mut buf = vec![];
        buf.push(msg.kind());
        let payload_data: Vec<u8> = match msg {
            DataMsg::Data(msg) => msg.into(),
            DataMsg::Query(msg) => msg.into(),
            DataMsg::RequestTail(msg) => msg.into(),
            DataMsg::PushPromise(msg) => msg.into(),
            DataMsg::Schema(msg) => msg.into(),
            DataMsg::CreateDb(msg) => msg.into(),
            DataMsg::CreateUser(msg) => msg.into(),
            DataMsg::RequestTailBatch(msg) => msg.into(),
            DataMsg::CredentialsResponse(msg) => msg.into(),
        };
        buf.extend(payload_data);

        buf
    }
}

#[derive(Debug)]
pub struct CloseMsg {}

impl CloseMsg {
    pub fn to_mux_msg(self, stream: u32) -> MuxMsg {
        MuxMsg {
            stream,
            payload: MuxMsgPayload::Close(self),
        }
    }
}

impl From<&[u8]> for CloseMsg {
    fn from(_data: &[u8]) -> Self {
        Self {}
    }
}

impl From<CloseMsg> for Vec<u8> {
    fn from(_msg: CloseMsg) -> Self {
        vec![]
    }
}

#[derive(Debug)]
pub struct ResetMsg {
    code: u32,
    msg: String,
}

impl ResetMsg {
    pub fn to_mux_msg(self, stream: u32) -> MuxMsg {
        MuxMsg {
            stream,
            payload: MuxMsgPayload::Reset(self),
        }
    }
}

impl From<&[u8]> for ResetMsg {
    fn from(data: &[u8]) -> Self {
        let code = u32::from_be_bytes(data[..4].try_into().unwrap());
        let data = &data[4..];

        let msg_len = u32::from_be_bytes(data[..4].try_into().unwrap()) as usize;
        let data = &data[4..];

        let msg = str::from_utf8(&data[..msg_len]).unwrap().to_string();

        Self { code, msg }
    }
}

impl From<ResetMsg> for Vec<u8> {
    fn from(msg: ResetMsg) -> Self {
        let mut buf = vec![];
        buf.extend(msg.code.to_be_bytes());
        buf.extend((msg.msg.len() as u32).to_be_bytes());
        buf.extend(msg.msg.as_bytes());

        buf
    }
}

#[derive(Debug)]
pub struct PingMsg {}

impl PingMsg {
    pub fn to_mux_msg(self, stream: u32) -> MuxMsg {
        MuxMsg {
            stream,
            payload: MuxMsgPayload::Ping(self),
        }
    }
}

impl From<&[u8]> for PingMsg {
    fn from(_data: &[u8]) -> Self {
        Self {}
    }
}

impl From<PingMsg> for Vec<u8> {
    fn from(_msg: PingMsg) -> Self {
        vec![]
    }
}

#[derive(Debug)]
pub struct PongMsg {}

impl PongMsg {
    pub fn to_mux_msg(self, stream: u32) -> MuxMsg {
        MuxMsg {
            stream,
            payload: MuxMsgPayload::Pong(self),
        }
    }
}

impl From<&[u8]> for PongMsg {
    fn from(_data: &[u8]) -> Self {
        Self {}
    }
}

impl From<PongMsg> for Vec<u8> {
    fn from(_msg: PongMsg) -> Self {
        vec![]
    }
}
