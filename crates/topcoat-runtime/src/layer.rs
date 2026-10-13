mod rerun;
#[cfg(not(target_family = "wasm"))]
mod socket;
pub mod transport;

pub use rerun::*;
use topcoat_core::context::Cx;
use topcoat_router::{Body, Layer, LayerFuture, Next, Path};
pub use transport::{Frame, PROTOCOL as RUNTIME_PROTOCOL};

/// A [`Layer`] that handles runtime requests at each page's URL.
///
/// The browser can request a render in two ways:
///
/// - Send a POST with X-Topcoat-Runtime: true and the document's signal values as JSON.  The layer
///   rewrites this to a GET at the same path and query.
/// - Open a WebSocket with the [`RUNTIME_PROTOCOL`] subprotocol.  Each render request on this
///   connection describes an HTTP request.  The layer dispatches it with the handshake's other
///   headers as a connected render.
///
/// WebSocket connections are supported on all platforms through the
/// [`transport`] module.  On native servers the layer accepts the
/// upgrade automatically; on other platforms (Cloudflare Workers, wasm)
/// use [`transport::serve_connection`] directly after accepting the
/// WebSocket.
///
/// Register this layer with
/// [`RouterBuilderRuntimeExt::runtime`](crate::RouterBuilderRuntimeExt::runtime).
#[derive(Debug, Clone, Copy, Default)]
pub struct RuntimeLayer;

impl Layer for RuntimeLayer {
    fn path(&self) -> Option<&Path> {
        None
    }

    fn handle<'a>(&'a self, cx: &'a Cx, body: Body, next: Next<'a>) -> LayerFuture<'a> {
        #[cfg(not(target_family = "wasm"))]
        if socket::requested(cx) {
            return Box::pin(socket::accept(cx, body));
        }
        if rerun::requested(cx) {
            return Box::pin(rerun::dispatch(cx, body));
        }
        next.run(cx, body)
    }
}

/// The limit set with
/// [`RouterBuilderRuntimeExt::max_runs_per_connection`](crate::RouterBuilderRuntimeExt::max_runs_per_connection).
#[derive(Debug, Clone, Copy)]
// Read only by the WebSocket handler, which native servers compile.
#[cfg_attr(target_family = "wasm", allow(dead_code))]
pub(crate) struct MaxRunsPerConnection(pub(crate) usize);
