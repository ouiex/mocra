//! Bounded shutdown for API connections, including unfinished request headers and bodies.
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

// axum spawns connection tasks separately from the server task. Wake their IO during forced
// shutdown so incomplete headers/bodies cannot keep sockets and server state alive.
pub(crate) struct ShutdownListener {
    pub listener: TcpListener,
    pub shutdown: watch::Receiver<()>,
}

impl axum::serve::Listener for ShutdownListener {
    type Io = ShutdownStream;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let (stream, addr) = axum::serve::Listener::accept(&mut self.listener).await;
        let mut shutdown = self.shutdown.clone();
        (
            ShutdownStream {
                stream: Some(stream),
                shutdown: Box::pin(async move {
                    let _ = shutdown.changed().await;
                }),
            },
            addr,
        )
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

pub(crate) struct ShutdownStream {
    stream: Option<TcpStream>,
    shutdown: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl ShutdownStream {
    fn poll_stream(&mut self, cx: &mut Context<'_>) -> Option<&mut TcpStream> {
        if self.stream.is_some() && self.shutdown.as_mut().poll(cx).is_ready() {
            self.stream.take();
        }
        self.stream.as_mut()
    }
}

impl AsyncRead for ShutdownStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.poll_stream(cx) {
            Some(stream) => Pin::new(stream).poll_read(cx, buf),
            None => Poll::Ready(Ok(())),
        }
    }
}

impl AsyncWrite for ShutdownStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.poll_stream(cx) {
            Some(stream) => Pin::new(stream).poll_write(cx, buf),
            None => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "HTTP server shut down",
            ))),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_stream(cx) {
            Some(stream) => Pin::new(stream).poll_flush(cx),
            None => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_stream(cx) {
            Some(stream) => Pin::new(stream).poll_shutdown(cx),
            None => Poll::Ready(Ok(())),
        }
    }
}
