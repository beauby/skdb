use std::str;

use crate::mux::*;

#[derive(Debug)]
pub struct Data {
    pub fin: bool,
    pub payload: Vec<u8>,
}

impl Data {
    pub fn to_mux_msg(self, stream: u32) -> MuxMsg {
        DataMsg::Data(self).to_mux_msg(stream)
    }
}

impl From<&[u8]> for Data {
    fn from(data: &[u8]) -> Self {
        let fin = data[0] != 0;
        let payload = data[1..].to_vec();

        Self { fin, payload }
    }
}

impl From<Data> for Vec<u8> {
    fn from(data: Data) -> Self {
        let mut buf = vec![];
        buf.extend((data.fin as u32).to_be_bytes());
        buf.extend(data.payload);

        buf
    }
}

#[derive(Debug)]
pub struct Query {
    pub format: QueryFormat,
    pub query: String,
}

#[derive(Debug)]
pub enum QueryFormat {
    JSON,
    Raw,
    CSV,
}

impl Query {
    pub fn to_mux_msg(self, stream: u32) -> MuxMsg {
        DataMsg::Query(self).to_mux_msg(stream)
    }
}

impl From<u8> for QueryFormat {
    fn from(b: u8) -> Self {
        match b {
            0x0 => QueryFormat::JSON,
            0x1 => QueryFormat::Raw,
            0x2 => QueryFormat::CSV,
            _ => panic!("Unknown query format {}", b),
        }
    }
}

impl From<QueryFormat> for u8 {
    fn from(format: QueryFormat) -> Self {
        match format {
            QueryFormat::JSON => 0x0,
            QueryFormat::Raw => 0x1,
            QueryFormat::CSV => 0x2,
        }
    }
}

impl From<&[u8]> for Query {
    fn from(data: &[u8]) -> Self {
        let data = &data[4..];

        let format: QueryFormat = data[0].into();
        let data = &data[1..];

        let query_len = u32::from_be_bytes(data[..4].try_into().unwrap()) as usize;
        let data = &data[4..];

        let query = str::from_utf8(&data[..query_len]).unwrap().to_string();

        Self { format, query }
    }
}

impl From<Query> for Vec<u8> {
    fn from(query: Query) -> Self {
        let mut buf = vec![];
        buf.push(query.format.into());
        buf.extend((query.query.len() as u32).to_be_bytes());
        buf.extend(query.query.as_bytes());

        buf
    }
}

#[derive(Debug)]
pub struct RequestTail {
    pub since: u64,
    pub table_name: String,
    pub schema: String,
    pub filter_expr: String,
    pub params_json: String,
}

impl RequestTail {
    pub fn new(
        since: u64,
        table_name: &str,
        schema: &str,
        filter_expr: &str,
        params_json: &str,
    ) -> Self {
        Self {
            since,
            table_name: table_name.to_string(),
            schema: schema.to_string(),
            filter_expr: filter_expr.to_string(),
            params_json: params_json.to_string(),
        }
    }

    pub fn to_mux_msg(self, stream: u32) -> MuxMsg {
        DataMsg::RequestTail(self).to_mux_msg(stream)
    }
}

impl From<&[u8]> for RequestTail {
    fn from(data: &[u8]) -> Self {
        parse_request_tail(data).0
    }
}

impl From<RequestTail> for Vec<u8> {
    fn from(request: RequestTail) -> Self {
        serialize_request_tail(request)
    }
}

#[derive(Debug)]
pub struct PushPromise {
    pub schemas: String,
}

impl PushPromise {
    pub fn to_mux_msg(self, stream: u32) -> MuxMsg {
        DataMsg::PushPromise(self).to_mux_msg(stream)
    }
}


impl From<&[u8]> for PushPromise {
    fn from(data: &[u8]) -> Self {
        let data = &data[3..];

        let schemas_len = u32::from_be_bytes(data[..4].try_into().unwrap()) as usize;
        let data = &data[4..];

        let schemas = str::from_utf8(&data[..schemas_len]).unwrap().to_string();

        Self { schemas }
    }
}

impl From<PushPromise> for Vec<u8> {
    fn from(promise: PushPromise) -> Self {
        let mut buf = vec![];
        buf.extend([0u8; 3]);
        buf.extend((promise.schemas.len() as u32).to_be_bytes());
        buf.extend(promise.schemas.as_bytes());

        buf
    }
}

#[derive(Debug)]
pub struct Schema {
    pub scope: SchemaScope,
    pub name: String,
    pub suffix: String,
}

#[derive(Debug)]
pub enum SchemaScope {
    All = 0,
    Table = 1,
    View = 2,
}

impl From<u8> for SchemaScope {
    fn from(b: u8) -> Self {
        match b {
            0 => SchemaScope::All,
            1 => SchemaScope::Table,
            2 => SchemaScope::View,
            _ => panic!("Unknown schema scope {}", b),
        }
    }
}

impl Schema {
    pub fn new(scope: SchemaScope, name: &str, suffix: &str) -> Self {
        Self {
            scope,
            name: name.to_string(),
            suffix: suffix.to_string(),
        }
    }

    pub fn to_mux_msg(self, stream: u32) -> MuxMsg {
        DataMsg::Schema(self).to_mux_msg(stream)
    }
}

impl From<&[u8]> for Schema {
    fn from(data: &[u8]) -> Self {
        let scope: SchemaScope = data[0].into();
        let data = &data[1..];

        let name_len = u16::from_be_bytes(data[..2].try_into().unwrap()) as usize;
        let data = &data[2..];

        let name = str::from_utf8(&data[..name_len]).unwrap().to_string();
        let data = &data[name_len..];

        let suffix_len = u16::from_be_bytes(data[..2].try_into().unwrap()) as usize;
        let data = &data[2..];

        let suffix = str::from_utf8(&data[..suffix_len]).unwrap().to_string();

        Self {
            scope,
            name,
            suffix,
        }
    }
}

impl From<Schema> for Vec<u8> {
    fn from(schema: Schema) -> Self {
        let mut buf = vec![];
        buf.push(schema.scope as u8);
        buf.extend((schema.name.len() as u16).to_be_bytes());
        buf.extend(schema.name.as_bytes());
        buf.extend((schema.suffix.len() as u16).to_be_bytes());
        buf.extend(schema.suffix.as_bytes());

        buf
    }
}

#[derive(Debug)]
pub struct CreateDb {
    pub name: String,
}

impl CreateDb {
    pub fn to_mux_msg(self, stream: u32) -> MuxMsg {
        DataMsg::CreateDb(self).to_mux_msg(stream)
    }
}

impl From<&[u8]> for CreateDb {
    fn from(data: &[u8]) -> Self {
        let name_len = u16::from_be_bytes(data[..2].try_into().unwrap()) as usize;
        let data = &data[2..];

        let name = str::from_utf8(&data[..name_len]).unwrap().to_string();
        Self { name }
    }
}

impl From<CreateDb> for Vec<u8> {
    fn from(create: CreateDb) -> Self {
        let mut buf = vec![];
        buf.extend((create.name.len() as u16).to_be_bytes());
        buf.extend(create.name.as_bytes());

        buf
    }
}

#[derive(Debug)]
pub struct CreateUser {}

impl CreateUser {
    pub fn to_mux_msg(self, stream: u32) -> MuxMsg {
        DataMsg::CreateUser(self).to_mux_msg(stream)
    }
}

impl From<&[u8]> for CreateUser {
    fn from(_data: &[u8]) -> Self {
        Self {}
    }
}

impl From<CreateUser> for Vec<u8> {
    fn from(_create: CreateUser) -> Self {
        vec![]
    }
}

#[derive(Debug)]
pub struct RequestTailBatch {
    pub requests: Vec<RequestTail>,
}

impl RequestTailBatch {
    pub fn to_mux_msg(self, stream: u32) -> MuxMsg {
        DataMsg::RequestTailBatch(self).to_mux_msg(stream)
    }
}

impl From<&[u8]> for RequestTailBatch {
    fn from(data: &[u8]) -> Self {
        let data = &data[1..];

        let batch_size = u16::from_be_bytes(data[..2].try_into().unwrap());
        let data = &data[2..];

        let mut requests = vec![];
        let mut pos = 0;
        for _ in 0..batch_size {
            pos += 1;
            let (req, len) = parse_request_tail(&data[pos..]);
            requests.push(req);
            pos += len;
        }

        RequestTailBatch { requests }
    }
}

impl From<RequestTailBatch> for Vec<u8> {
    fn from(batch: RequestTailBatch) -> Self {
        let mut buf = vec![];
        buf.push(0x0);
        buf.extend((batch.requests.len() as u16).to_be_bytes());
        for req in batch.requests {
            buf.push(0x2);
            buf.extend(serialize_request_tail(req))
        }

        buf
    }
}

#[derive(Debug)]
pub struct CredentialsResponse {
    pub access_key: String,
    pub private_key: [u8; 256],
}

impl CredentialsResponse {
    pub fn to_mux_msg(self, stream: u32) -> MuxMsg {
        DataMsg::CredentialsResponse(self).to_mux_msg(stream)
    }
}

impl From<&[u8]> for CredentialsResponse {
    fn from(data: &[u8]) -> Self {
        let access_key_len = data[..27].iter().position(|&c| c == 0).unwrap_or(27);
        let access_key = str::from_utf8(&data[..access_key_len]).unwrap().to_string();
        let data = &data[27..];

        let private_key = data[..32].try_into().unwrap();

        Self {
            access_key,
            private_key,
        }
    }
}

impl From<CredentialsResponse> for Vec<u8> {
    fn from(resp: CredentialsResponse) -> Self {
        let mut buf = vec![];
        buf.extend(format!("{:\0<27}", resp.access_key).as_bytes());
        buf.extend(resp.private_key);

        buf
    }
}

fn parse_request_tail(data: &[u8]) -> (RequestTail, usize) {
    let data = &data[3..];

    let since = u64::from_be_bytes(data[..8].try_into().unwrap());
    let data = &data[8..];

    let table_name_len = u16::from_be_bytes(data[..2].try_into().unwrap()) as usize;
    let data = &data[2..];

    let table_name = str::from_utf8(&data[..table_name_len]).unwrap().to_string();
    let data = &data[table_name_len..];

    let schema_len = u16::from_be_bytes(data[..2].try_into().unwrap()) as usize;
    let data = &data[2..];

    let schema = str::from_utf8(&data[..schema_len]).unwrap().to_string();
    let data = &data[schema_len..];

    let filter_expr_len = u16::from_be_bytes(data[..2].try_into().unwrap()) as usize;
    let data = &data[2..];

    let filter_expr = str::from_utf8(&data[..filter_expr_len])
        .unwrap()
        .to_string();
    let data = &data[filter_expr_len..];

    let params_json_len = u16::from_be_bytes(data[..2].try_into().unwrap()) as usize;
    let data = &data[2..];

    let params_json = str::from_utf8(&data[..params_json_len])
        .unwrap()
        .to_string();

    (
        RequestTail {
            since,
            table_name,
            schema,
            filter_expr,
            params_json,
        },
        19 + table_name_len + schema_len + filter_expr_len + params_json_len,
    )
}

fn serialize_request_tail(request: RequestTail) -> Vec<u8> {
    let mut buf = vec![];
    buf.extend([0u8; 3]);
    buf.extend(request.since.to_be_bytes());
    buf.extend((request.table_name.len() as u16).to_be_bytes());
    buf.extend(request.table_name.as_bytes());
    buf.extend((request.schema.len() as u16).to_be_bytes());
    buf.extend(request.schema.as_bytes());
    buf.extend((request.filter_expr.len() as u16).to_be_bytes());
    buf.extend(request.filter_expr.as_bytes());
    buf.extend((request.params_json.len() as u16).to_be_bytes());
    buf.extend(request.params_json.as_bytes());

    buf
}
