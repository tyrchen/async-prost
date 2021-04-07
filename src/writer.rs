use std::{
    io,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use byteorder::{NetworkEndian, WriteBytesExt};
use futures_core::ready;
use futures_sink::Sink;
use prost::Message;
use tokio::io::AsyncWrite;

/// A warpper around an async sink that accepts, serializes, and sends prost-encoded values.
#[derive(Debug)]
pub struct AsyncProstWriter<W, T, D> {
    writer: W,
    pub(crate) written: usize,
    pub(crate) buffer: Vec<u8>,
    from: PhantomData<T>,
    dest: PhantomData<D>,
}

impl<W, T, D> AsyncProstWriter<W, T, D> {
    /// create a new async prost writer
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            written: 0,
            buffer: Vec::new(),
            from: PhantomData,
            dest: PhantomData,
        }
    }

    /// Gets a reference to the underlying writer.
    pub fn get_ref(&self) -> &W {
        &self.writer
    }

    /// Gets a mutable reference to the underlying writer.
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// Unwraps this `AsyncProstWriter`, returning the underlying writer.
    ///
    /// Note that any leftover serialized data that has not yet been sent is lost.
    pub fn into_inner(self) -> W {
        self.writer
    }

    pub(crate) fn make_for<D2>(self) -> AsyncProstWriter<W, T, D2> {
        AsyncProstWriter {
            buffer: self.buffer,
            writer: self.writer,
            written: self.written,
            from: self.from,
            dest: PhantomData,
        }
    }
}

impl<W, T, D> Unpin for AsyncProstWriter<W, T, D> {}

impl<W, T> Default for AsyncProstWriter<W, T, SyncDestination>
where
    W: Default,
{
    fn default() -> Self {
        Self::from(W::default())
    }
}

impl<W, T> From<W> for AsyncProstWriter<W, T, SyncDestination> {
    fn from(writer: W) -> Self {
        Self::new(writer)
    }
}

impl<W, T> AsyncProstWriter<W, T, SyncDestination> {
    /// make this writer include the serialized data's size before each serialized value.
    pub fn for_async(self) -> AsyncProstWriter<W, T, AsyncDestination> {
        self.make_for()
    }
}

/// A marker that indicates that the wrapping type is compatible with `AsyncProstReader`.
#[derive(Debug)]
pub struct AsyncDestination;

/// A marker that indicates that the wrapping type is compatible with stock `prost` receivers.
#[derive(Debug)]
pub struct SyncDestination;

#[doc(hidden)]
pub trait ProstWriterFor<T> {
    fn append(&mut self, item: T) -> Result<(), io::Error>;
}

impl<W, T> ProstWriterFor<T> for AsyncProstWriter<W, T, AsyncDestination>
where
    T: Message,
{
    fn append(&mut self, item: T) -> Result<(), io::Error> {
        let size = item.encoded_len() as u32;
        self.buffer.write_u32::<NetworkEndian>(size)?;
        item.encode(&mut self.buffer)?;
        Ok(())
    }
}

// FIXME: why do we need this impl without writing the size?
impl<W, T> ProstWriterFor<T> for AsyncProstWriter<W, T, SyncDestination>
where
    T: Message,
{
    fn append(&mut self, item: T) -> Result<(), io::Error> {
        item.encode(&mut self.buffer)?;
        Ok(())
    }
}

impl<W, T, D> Sink<T> for AsyncProstWriter<W, T, D>
where
    T: Message,
    W: AsyncWrite + Unpin,
    Self: ProstWriterFor<T>,
{
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        if self.buffer.is_empty() {
            // NOTE: in theory we could have a short-circuit here that tries to have prost write
            // directly into self.writer. this would be way more efficient in the common case as we
            // don't have to do the extra buffering. the idea would be to serialize fist, and *if*
            // it errors, see how many bytes were written, serialize again into a Vec, and then
            // keep only the bytes following the number that were written in our buffer.
            // unfortunately, prost will not tell us that number at the moment, and instead just
            // fail.
        }

        self.append(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        // write stuff out if we need to
        while this.written != this.buffer.len() {
            let n =
                ready!(Pin::new(&mut this.writer).poll_write(cx, &this.buffer[this.written..]))?;
            this.written += n;
        }

        // we have to flush before we're really done
        this.buffer.clear();
        this.written = 0;
        Pin::new(&mut this.writer).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;
        Pin::new(&mut self.writer).poll_shutdown(cx)
    }
}
