
use async_std::stream::StreamExt;
use futures_util::sink::SinkExt;

#[derive(serde::Deserialize, Debug)]
pub struct OpenConnectionsResponse {
    pub ok: bool,
    pub url: Option<String>,
    pub error: Option<String>
}
pub async fn open_connections(token: &str) -> surf::Result<OpenConnectionsResponse> {
    surf::post("https://slack.com/api/apps.connections.open")
        .header(surf::http::headers::AUTHORIZATION, format!("Bearer {}", token))
        .recv_json().await
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum SocketModeMessage<'s> {
    Hello {  },
    Disconnect { reason: &'s str },
    EventsApi { envelope_id: &'s str }
}

#[derive(serde::Serialize)]
pub struct SocketModeAcknowledgeMessage<'s> {
    pub envelope_id: &'s str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<&'s str>
}

#[async_std::main]
async fn main() {
    dotenv::from_path(dotenv::dotenv().expect("Failed to get env file path")).expect("Failed to load env file");
    let tok = dotenv::var("SLACK_APP_TOKEN").expect("no SLACK_APP_TOKEN set");
    
    let con_result = open_connections(&tok).await.expect("Failed to request apps.connections.open");
    if !con_result.ok {
        panic!("app.connections.open failed: {}", con_result.error.as_deref().unwrap_or("Unknown error"));
    }
    let full_url = con_result.url.expect("no url passed from server");
    let url = url::Url::parse(&full_url).expect("Failed to parse entrypoint url");
    let domain = url.domain().expect("no domain name?");
    let tcp_stream = async_std::net::TcpStream::connect(&format!("{}:443", domain)).await.expect("Failed to connect tcp stream");
    let enc_stream = async_tls::TlsConnector::default().connect(domain, tcp_stream).await.expect("Failed to connect encrypted stream");
    let (mut stream, _) = async_tungstenite::client_async(full_url, enc_stream).await.expect("Failed to connect websocket");
    while let Some(m) = stream.next().await {
        match m.expect("Failed to decode websocket frame") {
            tungstenite::Message::Text(t) => match serde_json::from_str(&t) {
                Ok(SocketModeMessage::Hello { .. }) => { println!("Hello: {}", t); },
                Ok(SocketModeMessage::Disconnect { reason, .. }) => { println!("Disconnect request: {}", reason); break; },
                Ok(SocketModeMessage::EventsApi { envelope_id, .. }) => {
                    println!("Events API Message: {}", t);
                    stream.send(
                        tungstenite::Message::Text(
                            serde_json::to_string(
                                &SocketModeAcknowledgeMessage { envelope_id, payload: None }
                            ).expect("Failed to serialize ack message")
                        )
                    ).await.expect("Failed to reply ack message");
                }
                Err(e) => { println!("Unknown text frame: {}: {:?}", t, e); }
            },
            tungstenite::Message::Ping(bytes) => {
                println!("ping: {:?}", bytes);
            },
            _ => println!("Unknown frame")
        }
    }
}
