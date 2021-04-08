use std::{
    fmt, io,
    ops::{Deref, DerefMut},
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::Stream;
use futures_sink::Sink;
use tokio::{
    io::{AsyncRead, ReadBuf},
    net::{
        tcp::{ReadHalf, WriteHalf},
        TcpStream,
    },
};

use crate::{
    AsyncDestination, AsyncFrameDestination, AsyncProstReader, AsyncProstWriter, SyncDestination,
};

/// A wrapper around an async stream that receives and sends prost-encoded values
#[derive(Debug)]
pub struct AsyncProstStream<S, R, W, D> {
    stream: AsyncProstReader<InternalAsyncWriter<S, W, D>, R, D>,
}

#[doc(hidden)]
pub struct InternalAsyncWriter<S, T, D>(AsyncProstWriter<S, T, D>);

impl<S: fmt::Debug, T, D> fmt::Debug for InternalAsyncWriter<S, T, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get_ref().fmt(f)
    }
}

impl<S, T, D> Deref for InternalAsyncWriter<S, T, D> {
    type Target = AsyncProstWriter<S, T, D>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<S, T, D> DerefMut for InternalAsyncWriter<S, T, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<S, R, W> Default for AsyncProstStream<S, R, W, SyncDestination>
where
    S: Default,
{
    fn default() -> Self {
        Self::from(S::default())
    }
}

impl<S, R, W> From<S> for AsyncProstStream<S, R, W, SyncDestination> {
    fn from(stream: S) -> Self {
        Self {
            stream: AsyncProstReader::from(InternalAsyncWriter(AsyncProstWriter::from(stream))),
        }
    }
}

impl<S, R, W, D> AsyncProstStream<S, R, W, D> {
    /// Gets a reference to the underlying stream.
    ///
    /// It is inadvisable to directly read from or write to the underlying stream.
    pub fn get_ref(&self) -> &S {
        &self.stream.get_ref().0.get_ref()
    }

    /// Gets a mutable reference to the underlying stream.
    ///
    /// It is inadvisable to directly read from or write to the underlying stream.
    pub fn get_mut(&mut self) -> &mut S {
        self.stream.get_mut().0.get_mut()
    }

    /// Unwraps this `AsyncProstStream`, returning the underlying stream.
    ///
    /// Note that any leftover serialized data that has not yet been sent, or received data that
    /// has not yet been deserialized, is lost.
    pub fn into_inner(self) -> S {
        self.stream.into_inner().0.into_inner()
    }
}

impl<S, R, W, D> AsyncProstStream<S, R, W, D> {
    /// make this stream include the serialized data's size before each serialized value
    pub fn for_async(self) -> AsyncProstStream<S, R, W, AsyncDestination> {
        let stream = self.into_inner();
        AsyncProstStream {
            stream: AsyncProstReader::from(InternalAsyncWriter(
                AsyncProstWriter::from(stream).for_async(),
            )),
        }
    }

    /// make this stream include the serialized data's size before each serialized value
    pub fn for_async_framed(self) -> AsyncProstStream<S, R, W, AsyncFrameDestination> {
        let stream = self.into_inner();
        AsyncProstStream {
            stream: AsyncProstReader::from(InternalAsyncWriter(
                AsyncProstWriter::from(stream).for_async_framed(),
            )),
        }
    }

    /// Make this stream only send prost-encoded values
    pub fn for_sync(self) -> AsyncProstStream<S, R, W, SyncDestination> {
        AsyncProstStream::from(self.into_inner())
    }
}

impl<R, W, D> AsyncProstStream<TcpStream, R, W, D> {
    /// split a TCP-based stream into a read half and a write half
    pub fn tcp_split(
        &mut self,
    ) -> (
        AsyncProstReader<ReadHalf, R, D>,
        AsyncProstWriter<WriteHalf, W, D>,
    ) {
        // first, steal the reader state so it isn't lost
        let rbuff = self.stream.buffer.split();
        // then fish out the writer
        let writer = &mut self.stream.get_mut().0;
        // and steal the writer state so it isn't lost
        let wbuff = writer.buffer.split_off(0);
        let wsize = writer.written;
        // now split the stream
        let (r, w) = writer.get_mut().split();
        // then put the reader back together
        let mut reader = AsyncProstReader::from(r);
        reader.buffer = rbuff;
        // and then writer
        let mut writer = AsyncProstWriter::from(w).make_for();
        writer.buffer = wbuff;
        writer.written = wsize;

        (reader, writer)
    }
}

impl<S, T, D> AsyncRead for InternalAsyncWriter<S, T, D>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(self.get_mut().get_mut()).poll_read(cx, buf)
    }
}

impl<S, R, W, D> Stream for AsyncProstStream<S, R, W, D>
where
    S: Unpin,
    AsyncProstReader<InternalAsyncWriter<S, W, D>, R, D>: Stream<Item = Result<R, io::Error>>,
{
    type Item = Result<R, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

impl<S, R, W, D> Sink<W> for AsyncProstStream<S, R, W, D>
where
    S: Unpin,
    AsyncProstWriter<S, W, D>: Sink<W, Error = io::Error>,
{
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut **self.stream.get_mut()).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: W) -> Result<(), Self::Error> {
        Pin::new(&mut **self.stream.get_mut()).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut **self.stream.get_mut()).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut **self.stream.get_mut()).poll_close(cx)
    }
}
