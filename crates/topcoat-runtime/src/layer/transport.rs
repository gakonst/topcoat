//! Platform-independent runtime socket transport.
//!
//! [`serve_connection`] runs the runtime protocol over any WebSocket
//! transport that provides a [`Frame`] stream and sink.  The native server
//! path uses this under the hood; call it directly for platforms that
//! accept WebSockets without hyper upgrades, such as Cloudflare Workers
//! using `WebSocketPair`.

use std::{collections::HashMap, sync::Arc};

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use topcoat_router::{
    Body, HeaderMap, HeaderName, HeaderValue, Method, RemoteAddr, Router, Uri,
    content::ViewResponseDelivery,
    header,
    request::{IDENTITY_HEADER, Request},
};

use crate::{ConnectedRender, RUNTIME_HEADER};

/// How many runs one connection may have at once by default.
const DEFAULT_MAX_RUNS_PER_CONNECTION: usize = 64;

/// One WebSocket frame, independent of the hosting platform.
#[derive(Debug, Clone)]
pub enum Frame {
    /// A UTF-8 text frame.
    Text(String),
    /// A binary frame.
    Binary(Vec<u8>),
    /// A close frame.  The connection should be shut down.
    Close,
}

/// The runtime subprotocol name.
pub const PROTOCOL: &str = "topcoat-runtime";

/// Returns true for a GET whose Sec-WebSocket-Protocol header offers PROTOCOL.
pub fn is_runtime_upgrade(parts: &http::request::Parts) -> bool {
    parts.method == Method::GET && requests_runtime_protocol(&parts.headers)
}

/// Serves the runtime protocol over an already-accepted socket until it
/// closes.
///
/// Runs on one connection execute concurrently: each render request is
/// dispatched in its own task (`tokio::spawn` on native,
/// `wasm_bindgen_futures::spawn_local` on wasm).  Stop messages and
/// disconnects abort in-flight renders.
///
/// The maximum number of concurrent runs defaults to 64.
pub async fn serve_connection<I, O>(
    router: Router,
    handshake: http::request::Parts,
    incoming: I,
    outgoing: O,
) where
    I: futures_core::Stream<Item = Frame> + Unpin + 'static,
    O: futures_sink::Sink<Frame> + Unpin + 'static,
{
    serve_impl(
        router,
        handshake,
        incoming,
        outgoing,
        DEFAULT_MAX_RUNS_PER_CONNECTION,
    )
    .await;
}

pub(crate) async fn serve_impl<I, O>(
    router: Router,
    handshake: http::request::Parts,
    incoming: I,
    outgoing: O,
    max_runs: usize,
) where
    I: futures_core::Stream<Item = Frame> + Unpin + 'static,
    O: futures_sink::Sink<Frame> + Unpin + 'static,
{
    let connection = Arc::new(Connection::from_parts(router, &handshake));
    let (sender, receiver) = futures_channel::mpsc::channel::<Frame>(16);

    let forward = forward_frames(receiver, outgoing);
    let receive = receive_loop(connection, incoming, sender, max_runs);

    futures_util::future::join(forward, receive).await;
}

async fn forward_frames<O>(mut receiver: futures_channel::mpsc::Receiver<Frame>, mut outgoing: O)
where
    O: futures_sink::Sink<Frame> + Unpin,
{
    while let Some(frame) = receiver.next().await {
        if outgoing.send(frame).await.is_err() {
            break;
        }
    }
}

async fn receive_loop<I>(
    connection: Arc<Connection>,
    mut incoming: I,
    sender: futures_channel::mpsc::Sender<Frame>,
    max_runs: usize,
) where
    I: futures_core::Stream<Item = Frame> + Unpin,
{
    let mut runs = HashMap::<u64, RunHandle>::new();

    while let Some(frame) = incoming.next().await {
        let text = match frame {
            Frame::Text(text) => text,
            Frame::Close => break,
            Frame::Binary(_) => continue,
        };

        let request = match serde_json::from_str::<ClientMessage>(&text) {
            Ok(ClientMessage::Run(request)) => request,
            Ok(ClientMessage::Stop { stop }) => {
                if let Some(run) = runs.remove(&stop) {
                    run.abort();
                }
                continue;
            }
            Err(_) => {
                let error = ConnectionFrame::Error { status: 400 }.to_frame(None);
                if sender.clone().send(error).await.is_err() {
                    break;
                }
                continue;
            }
        };

        runs.retain(|_, run| !run.is_finished());
        let id = request.run;

        if !runs.contains_key(&id) && runs.len() >= max_runs {
            let error = ConnectionFrame::Error { status: 429 }.to_frame(Some(id));
            if sender.clone().send(error).await.is_err() {
                break;
            }
            continue;
        }

        let conn = Arc::clone(&connection);
        let out = sender.clone();
        let handle = spawn_run(async move {
            conn.render(request, out).await;
        });

        if let Some(previous) = runs.insert(id, handle) {
            previous.abort();
        }
    }

    for run in runs.values() {
        run.abort();
    }
}

pub(crate) fn requests_runtime_protocol(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|protocol| protocol.trim() == PROTOCOL)
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ClientMessage {
    Stop { stop: u64 },
    Run(RunRequest),
}

#[derive(Debug, Deserialize)]
struct RunRequest {
    run: u64,
    method: String,
    path: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    body: String,
}

#[derive(Serialize)]
#[serde(tag = "t", rename_all = "snake_case")]
enum ConnectionFrame<'a> {
    Redirect { location: &'a str },
    Error { status: u16 },
}

impl ConnectionFrame<'_> {
    fn to_frame(&self, run: Option<u64>) -> Frame {
        let json = serde_json::to_string(self).expect("a connection frame serializes");
        envelope(run, &json)
    }
}

fn envelope(run: Option<u64>, frame: &str) -> Frame {
    let frame = frame.trim_end();
    Frame::Text(match run {
        Some(run) => format!("{{\"run\":{run},\"frame\":{frame}}}"),
        None => format!("{{\"frame\":{frame}}}"),
    })
}

fn allowed_header(name: &HeaderName) -> bool {
    *name == header::CONTENT_TYPE || name == IDENTITY_HEADER || *name == RUNTIME_HEADER
}

struct Connection {
    router: Router,
    headers: HeaderMap,
    remote: Option<RemoteAddr>,
}

impl Connection {
    fn from_parts(router: Router, handshake: &http::request::Parts) -> Self {
        let mut headers = handshake.headers.clone();
        for name in [
            header::CONNECTION,
            header::UPGRADE,
            header::SEC_WEBSOCKET_KEY,
            header::SEC_WEBSOCKET_VERSION,
            header::SEC_WEBSOCKET_PROTOCOL,
            header::SEC_WEBSOCKET_EXTENSIONS,
            header::ACCEPT_ENCODING,
        ] {
            headers.remove(name);
        }
        Self {
            router,
            headers,
            remote: handshake.extensions.get::<RemoteAddr>().copied(),
        }
    }

    fn request(&self, run: RunRequest) -> Option<Request> {
        let method = Method::from_bytes(run.method.as_bytes()).ok()?;
        let uri = Uri::try_from(run.path).ok()?;
        if uri.scheme().is_some() || !uri.path().starts_with('/') {
            return None;
        }
        let mut headers = self.headers.clone();
        for (name, value) in run.headers {
            let name = HeaderName::try_from(name).ok()?;
            if !allowed_header(&name) {
                return None;
            }
            headers.insert(name, HeaderValue::try_from(value).ok()?);
        }

        let mut request = Request::new(Body::from(run.body));
        *request.method_mut() = method;
        *request.uri_mut() = uri;
        *request.headers_mut() = headers;
        if let Some(remote) = self.remote {
            request.extensions_mut().insert(remote);
        }
        Some(request)
    }

    async fn render(&self, run: RunRequest, mut out: futures_channel::mpsc::Sender<Frame>) {
        let id = Some(run.run);
        let Some(request) = self.request(run) else {
            let error = ConnectionFrame::Error { status: 400 };
            let _ = out.send(error.to_frame(id)).await;
            return;
        };
        let response = self
            .router
            .handle_with(request, (ConnectedRender, ViewResponseDelivery::Frames))
            .await;

        let status = response.status();
        if status.is_redirection()
            && let Some(location) = response.headers().get(header::LOCATION)
            && let Ok(location) = location.to_str()
        {
            let redirect = ConnectionFrame::Redirect { location };
            let _ = out.send(redirect.to_frame(id)).await;
            return;
        }

        let has_frames = response
            .headers()
            .get(header::CONTENT_TYPE)
            .is_some_and(|value| value == "application/x-ndjson");
        if !status.is_success() || !has_frames {
            let error = ConnectionFrame::Error {
                status: status.as_u16(),
            };
            let _ = out.send(error.to_frame(id)).await;
            return;
        }

        let mut frames = response.into_body().into_data_stream();
        while let Some(frame) = frames.next().await {
            let message = match frame.as_deref().map(std::str::from_utf8) {
                Ok(Ok(text)) => envelope(id, text),
                _ => ConnectionFrame::Error { status: 500 }.to_frame(id),
            };
            if out.send(message).await.is_err() {
                return;
            }
        }
    }
}

#[cfg(not(target_family = "wasm"))]
struct RunHandle(tokio::task::JoinHandle<()>);

#[cfg(not(target_family = "wasm"))]
impl RunHandle {
    fn abort(&self) {
        self.0.abort();
    }
    fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}

#[cfg(not(target_family = "wasm"))]
fn spawn_run<F>(fut: F) -> RunHandle
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    RunHandle(tokio::spawn(fut))
}

#[cfg(target_family = "wasm")]
struct RunHandle {
    abort: futures_util::future::AbortHandle,
    finished: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(target_family = "wasm")]
impl RunHandle {
    fn abort(&self) {
        self.abort.abort();
    }
    fn is_finished(&self) -> bool {
        self.finished.load(std::sync::atomic::Ordering::Acquire)
    }
}

#[cfg(target_family = "wasm")]
fn spawn_run<F>(fut: F) -> RunHandle
where
    F: std::future::Future<Output = ()> + 'static,
{
    let (abort_handle, abort_reg) = futures_util::future::AbortHandle::new_pair();
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let finished_clone = Arc::clone(&finished);
    wasm_bindgen_futures::spawn_local(async move {
        let _ = futures_util::future::Abortable::new(fut, abort_reg).await;
        finished_clone.store(true, std::sync::atomic::Ordering::Release);
    });
    RunHandle {
        abort: abort_handle,
        finished,
    }
}

#[cfg(test)]
mod tests {
    use topcoat_router::HeaderValue;

    use super::*;

    fn headers_with_protocols(value: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static(value),
        );
        headers
    }

    #[test]
    fn the_runtime_protocol_is_recognized_among_others() {
        assert!(requests_runtime_protocol(&headers_with_protocols(
            "topcoat-runtime"
        )));
        assert!(requests_runtime_protocol(&headers_with_protocols(
            "chat.v1, topcoat-runtime"
        )));
        assert!(!requests_runtime_protocol(&headers_with_protocols(
            "topcoat-runtime-v2"
        )));
        assert!(!requests_runtime_protocol(&HeaderMap::new()));
    }

    #[test]
    fn is_runtime_upgrade_checks_method_and_protocol() {
        let mut parts = http::Request::builder()
            .method("GET")
            .uri("/")
            .header("sec-websocket-protocol", "topcoat-runtime")
            .body(())
            .unwrap()
            .into_parts()
            .0;
        assert!(is_runtime_upgrade(&parts));
        parts.method = Method::POST;
        assert!(!is_runtime_upgrade(&parts));
    }
}
