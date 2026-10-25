//! 覆盖一次往返的地址、认证头与事件流处理

use futures::StreamExt;
use std::io;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wind_ai::adapter::{ChatAdapter, adapter_of};
use wind_ai::chat::{Chunk, Request, once, stream};
use wind_ai::model::{AdapterType, ChatModel, Target};
use wind_wire::message::{Content, Message, Role};

#[tokio::test]
async fn the_url_is_the_base_plus_the_adapters_path() {
    let server = Server::serve("data: {\"choices\":[]}\n\ndata: [DONE]\n\n").await;
    let request = Request {
        model: model(),
        target: Target {
            base_url: server.base_url(),
            api_key: Some("sk-test".to_string()),
        },
        contexts: vec![Message {
            role: Role::User,
            content: vec![Content::new_text("hello".into())],
            ..Message::default()
        }],
        tools: None,
    };

    let mut chunks = Box::pin(stream(adapter(), &request).await.expect("send request"));
    let _ = chunks.next().await;

    let seen = server.seen();
    assert_eq!(
        seen[0].0, "/chat/completions",
        "the address is the base plus the path supplied by the adapter"
    );
    assert_eq!(
        seen[0].1.as_deref(),
        Some("Bearer sk-test"),
        "the credential is injected by the host into the auth header"
    );
}

#[tokio::test]
async fn a_custom_endpoint_overrides_the_adapters_path() {
    let server = Server::serve("data: [DONE]\n\n").await;
    let mut model = model();
    model.endpoint = Some("/v2/chat".to_string());
    let request = Request {
        model,
        target: Target {
            base_url: server.base_url(),
            api_key: None,
        },
        contexts: Vec::new(),
        tools: None,
    };
    let _ = stream(adapter(), &request).await.expect("send request");
    assert_eq!(server.seen()[0].0, "/v2/chat");
}

/// 构造一个指向某个服务的请求
fn request_to(server: &Server, endpoint: Option<&str>) -> Request {
    Request {
        model: ChatModel {
            endpoint: endpoint.map(str::to_string),
            ..model()
        },
        target: Target {
            base_url: server.base_url(),
            api_key: None,
        },
        contexts: Vec::new(),
        tools: None,
    }
}

#[tokio::test]
async fn the_stream_ends_with_done() {
    let server = Server::serve(
        "data: {\"id\":\"x\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"world\"}}]}\n\ndata: [DONE]\n\n",
    )
    .await;
    let request = request_to(&server, None);
    let chunks: Vec<_> = stream(adapter(), &request)
        .await
        .expect("send request")
        .collect()
        .await;
    assert!(matches!(
        chunks.last(),
        Some(Ok(wind_ai::chat::Chunk::Done))
    ));
}

#[tokio::test]
async fn a_successive_chunk_carries_the_delta() {
    let server = Server::serve(
        "data: {\"id\":\"x\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"world\"}}]}\n\ndata: [DONE]\n\n",
    )
    .await;
    let request = request_to(&server, None);
    let chunks: Vec<_> = stream(adapter(), &request)
        .await
        .expect("send request")
        .collect()
        .await;
    let first = chunks[0].as_ref().expect("parse");
    let Chunk::Delta(message) = first else {
        panic!("expected a delta chunk")
    };
    assert_eq!(message.content.len(), 1);
}

#[tokio::test]
async fn a_non_success_status_becomes_an_error_carrying_the_body() {
    let server = Server::failing(500, r#"{"error":{"message":"boom"}}"#).await;
    let error = stream(adapter(), &request_to(&server, None))
        .await
        .err()
        .expect("must fail");
    assert!(error.to_string().contains("boom"), "{error}");
}

#[tokio::test]
async fn once_returns_the_whole_message() {
    let server = Server::serve(
        r#"{"id":"x","object":"chat.completion","created":1,"model":"m",
            "choices":[{"index":0,"message":{"role":"assistant","content":"hello"},
                        "finish_reason":"stop"}]}"#,
    )
    .await;
    let message = once(adapter(), &request_to(&server, None))
        .await
        .expect("send request");
    assert_eq!(message.content.len(), 1);
}

/// 构造一个走 chat completions 的模型描述
fn model() -> ChatModel {
    ChatModel {
        name: "deepseek-chat".to_string(),
        adapter: AdapterType::OpenAICompletion,
        endpoint: None,
        config: Some(serde_json::json!({ "stream": true })),
    }
}

/// 一个走 chat completions 接口的适配器
fn adapter() -> &'static dyn ChatAdapter {
    // 适配器不带状态，全部用例共用一个实例
    static ADAPTER: OnceLock<Box<dyn ChatAdapter>> = OnceLock::new();
    let adapter = ADAPTER.get_or_init(|| adapter_of(AdapterType::OpenAICompletion));
    &**adapter
}

/// 收到的请求记录，每项含路径与认证头
type Seen = Vec<(String, Option<String>)>;

/// 一个只回一段固定响应的本地服务
///
/// 它记下收到的请求，供用例断言地址与头
struct Server {
    /// 收到过的请求，含路径与认证头
    seen: Arc<Mutex<Seen>>,
    /// 监听的基地址
    base_url: String,
}

impl Server {
    /// 起一个回 200 与给定 body 的服务
    async fn serve(body: &'static str) -> Self {
        Self::start(200, body).await
    }

    /// 起一个回给定状态码与 body 的服务
    async fn failing(status: u16, body: &'static str) -> Self {
        Self::start(status, body).await
    }

    /// 服务的基地址
    fn base_url(&self) -> String {
        self.base_url.clone()
    }

    /// 收到过的请求，含路径与认证头
    fn seen(&self) -> Seen {
        self.seen.lock().expect("lock the mutex").clone()
    }

    /// 起服务并逐个处理连接
    ///
    /// 只服务本文件的用例。响应不分块、不使用 keep-alive，写完就关闭连接
    async fn start(status: u16, body: &'static str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listen");
        let base_url = format!(
            "http://{}",
            listener.local_addr().expect("read the local address")
        );
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&seen);
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                if let Some(request) = read_request(&mut socket).await {
                    recorded.lock().expect("lock the mutex").push(request);
                }
                let _ = respond(&mut socket, status, body).await;
            }
        });
        Self { seen, base_url }
    }
}

/// 读一次请求，返回路径与认证头
///
/// 按 `Content-Length` 读完请求体再返回，避免在对方仍在发送时关闭连接
async fn read_request(socket: &mut TcpStream) -> Option<(String, Option<String>)> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        if let Some(end) = head_end(&buffer) {
            let head = String::from_utf8_lossy(&buffer[..end]).into_owned();
            if buffer.len() >= end + body_len(&head) {
                let mut lines = head.lines();
                let request_line = lines.next()?;
                let path = request_line.split_whitespace().nth(1)?.to_string();
                let authorization = lines.find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("authorization")
                        .then(|| value.trim().to_string())
                });
                return Some((path, authorization));
            }
        }
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
}

/// 头部结束的位置，即空行之后的偏移
fn head_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|at| at + 4)
}

/// 头部里声明的请求体长度
fn body_len(head: &str) -> usize {
    head.lines()
        .filter_map(|line| line.split_once(':'))
        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .find_map(|(_, value)| value.trim().parse().ok())
        .unwrap_or(0)
}

/// 把固定的一段 body 写回去
async fn respond(socket: &mut TcpStream, status: u16, body: &str) -> io::Result<()> {
    let reason = if status == 200 { "OK" } else { "Error" };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(body.as_bytes()).await?;
    socket.flush().await?;
    socket.shutdown().await
}
