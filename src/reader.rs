use byteorder::{ByteOrder, NetworkEndian};
use std::{
    io,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, BytesMut};
use futures_core::{ready, Stream};
use prost::Message;
use tokio::io::{AsyncRead, ReadBuf};

const BUFFER_SIZE: usize = 8192;
const LEN_SIZE: usize = 4;

enum FillResult {
    Filled,
    Eof,
}

/// A wrapper around an async reader that produces an asynchronous stream of prost-decoded values
#[derive(Debug)]
pub struct AsyncProstReader<R, T> {
    reader: R,
    pub(crate) buffer: BytesMut,
    into: PhantomData<T>,
}
impl<R, T> Unpin for AsyncProstReader<R, T> where R: Unpin {}

impl<R, T> AsyncProstReader<R, T> {
    /// create a new reader
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
            into: PhantomData,
        }
    }

    /// gets a reference to the underlying reader
    pub fn get_ref(&self) -> &R {
        &self.reader
    }

    /// gets a mutable reference to the underlying reader
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    /// returns a reference to the internally buffered data
    pub fn buffer(&self) -> &[u8] {
        &self.buffer[..]
    }

    /// unwrap the `AsyncProstReader`, returning the underlying reader
    pub fn into_inner(self) -> R {
        self.reader
    }
}

impl<R, T> Default for AsyncProstReader<R, T>
where
    R: Default,
{
    fn default() -> Self {
        Self::from(R::default())
    }
}

impl<R, T> From<R> for AsyncProstReader<R, T> {
    fn from(reader: R) -> Self {
        Self::new(reader)
    }
}

impl<R, T> Stream for AsyncProstReader<R, T>
where
    T: Message + Default,
    R: AsyncRead + Unpin,
{
    type Item = Result<T, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // FIXME: what 5 means here?
        if let FillResult::Eof = ready!(self.as_mut().fill(cx, 5))? {
            return Poll::Ready(None);
        }

        let message_size = NetworkEndian::read_u32(&self.buffer[..LEN_SIZE]) as usize;

        // since self.buffer.len() >= 4, we know that we can't get a clean EOF here
        ready!(self.as_mut().fill(cx, message_size + LEN_SIZE))?;

        self.buffer.advance(LEN_SIZE);
        let message =
            Message::decode(&self.buffer[..message_size]).map_err(prost::DecodeError::from)?;
        self.buffer.advance(message_size);
        Poll::Ready(Some(Ok(message)))
    }
}

impl<R, T> AsyncProstReader<R, T>
where
    T: Message + Default,
    R: AsyncRead + Unpin,
{
    fn fill(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        target_buffer_size: usize,
    ) -> Poll<Result<FillResult, io::Error>> {
        if self.buffer.len() >= target_buffer_size {
            // we already ave the bytes we need!
            return Poll::Ready(Ok(FillResult::Filled));
        }

        // make sure we can fit all the data we're about to read
        if self.buffer.capacity() < target_buffer_size {
            let missing = target_buffer_size - self.buffer.capacity();
            self.buffer.reserve(missing);
        }

        let had = self.buffer.len();
        // this is the bit we'll be reading into
        let mut rest = self.buffer.split_off(had);
        // this is safe because we're not extending beyond the reserved capacity
        // and we're never reading unwritten bytes
        let max = rest.capacity();
        unsafe { rest.set_len(max) };

        while self.buffer.len() < target_buffer_size {
            let mut buf = ReadBuf::new(&mut rest[..]);
            ready!(Pin::new(&mut self.reader).poll_read(cx, &mut buf))?;
            let n = buf.filled().len();
            if n == 0 {
                if self.buffer.is_empty() {
                    return Poll::Ready(Ok(FillResult::Eof));
                } else {
                    return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)));
                }
            }

            // adopt the new bytes
            let read = rest.split_to(n);
            self.buffer.unsplit(read);
        }

        Poll::Ready(Ok(FillResult::Filled))
    }
}
