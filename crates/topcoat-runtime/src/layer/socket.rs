//! Native WebSocket adapter for the runtime socket transport.

use futures_util::{SinkExt, StreamExt};
use topcoat_core::{
    context::{Cx, try_app_context},
    error::Result,
};
use topcoat_router::{
    Body, Method, RemoteAddr,
    content::websocket::{Message, WebSocketUpgrade},
    request::{FromRequest, extensions, headers, method},
    response::Response,
    router,
};

use super::{
    MaxRunsPerConnection,
    transport::{self, Frame, PROTOCOL},
};

const DEFAULT_MAX_RUNS_PER_CONNECTION: usize = 64;

/// Checks for a GET that requests the runtime WebSocket subprotocol.
pub(super) fn requested(cx: &Cx) -> bool {
    *method(cx) == Method::GET && transport::requests_runtime_protocol(headers(cx))
}

/// Opens the WebSocket and starts handling render requests.
pub(super) async fn accept(cx: &Cx, body: Body) -> Result<Response> {
    let upgrade = WebSocketUpgrade::from_request(cx, body).await?;
    let max_runs = try_app_context::<MaxRunsPerConnection>(cx)
        .map_or(DEFAULT_MAX_RUNS_PER_CONNECTION, |max| max.0);

    // Build handshake parts for the transport layer.
    let handshake = {
        let mut req = http::Request::builder()
            .method(method(cx).clone())
            .uri(topcoat_router::request::uri(cx).clone());
        for (name, value) in headers(cx) {
            req = req.header(name, value);
        }
        let (mut parts, ()) = req.body(()).unwrap().into_parts();
        if let Some(&remote) = extensions(cx).get::<RemoteAddr>() {
            parts.extensions.insert(remote);
        }
        parts
    };

    let router = router(cx);

    upgrade
        .protocols([PROTOCOL])
        .on_upgrade(move |socket| async move {
            let (ws_sink, ws_stream) = socket.split();

            let incoming = Box::pin(ws_stream.filter_map(|result| async move {
                match result {
                    Ok(Message::Text(text)) => Some(Frame::Text(text.to_string())),
                    Ok(Message::Binary(data)) => Some(Frame::Binary(data.to_vec())),
                    Ok(Message::Close(_)) => Some(Frame::Close),
                    _ => None,
                }
            }));

            let outgoing = Box::pin(ws_sink.with(|frame: Frame| async move {
                Ok::<_, topcoat_core::error::Error>(match frame {
                    Frame::Text(text) => Message::text(text),
                    Frame::Binary(data) => Message::binary(data),
                    Frame::Close => Message::Close(None),
                })
            }));

            transport::serve_impl(router, handshake, incoming, outgoing, max_runs).await;
        })
}
