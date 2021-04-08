use bytes::BufMut;
use core::fmt::Debug;
use either::Either;
use prost::Message;
use std::io::{self};

#[derive(Debug)]
/// Decoded frame from buffer
pub struct Frame<H, T> {
    /// header of the frame
    pub header: Option<H>,
    /// body of the frame
    pub body: Option<Either<Vec<u8>, T>>,
}

impl<H, T> Default for Frame<H, T> {
    fn default() -> Self {
        Self {
            header: None,
            body: None,
        }
    }
}

/// indicate if we shall decode body or not
pub trait ShallDecodeBody {
    /// return true if decode body is required
    fn shall_decode_body(&self) -> bool;
}

/// encode and decode for frame
pub trait Framed: Debug + Send + Sync {
    /// decode header(if exists) and body
    fn decode(buf: &[u8], header_len: usize) -> Result<Self, io::Error>
    where
        Self: Default;

    /// encoded length
    fn encoded_len(&self) -> u32
    where
        Self: Sized;

    /// encode header and body, with length
    fn encode<B>(&self, buf: &mut B) -> Result<(), io::Error>
    where
        B: BufMut,
        Self: Sized;
}

impl<H, T> Framed for Frame<H, T>
where
    H: Message + ShallDecodeBody + Default,
    T: Message + Default,
{
    fn decode(buf: &[u8], header_len: usize) -> Result<Self, io::Error>
    where
        Self: Default,
    {
        let mut this = Self::default();
        let decode_body;
        if header_len > 0 {
            let header = H::decode(&buf[0..header_len])?;
            decode_body = header.shall_decode_body();
            this.header = Some(header);
        } else {
            this.header = Some(H::default());
            decode_body = true;
        }

        let body_buf = &buf[header_len..];
        if decode_body {
            let msg = Message::decode(body_buf)?;

            this.body = Some(Either::Right(msg));
        } else {
            let data = body_buf.to_vec();
            this.body = Some(Either::Left(data));
        }

        Ok(this)
    }

    fn encoded_len(&self) -> u32
    where
        Self: Sized,
    {
        let header_len = if let Some(header) = self.header.as_ref() {
            header.encoded_len() as u8
        } else {
            0
        };
        let body_len = match self.body.as_ref() {
            Some(Either::Left(v)) => v.len() as u32,
            Some(Either::Right(v)) => v.encoded_len() as u32,
            None => 0,
        };

        (header_len as u32) << 24 | body_len
    }

    fn encode<B>(&self, buf: &mut B) -> Result<(), io::Error>
    where
        B: BufMut,
        Self: Sized,
    {
        if let Some(header) = self.header.as_ref() {
            header.encode(buf)?;
        }

        match self.body.as_ref() {
            Some(Either::Left(v)) => {
                buf.put(v.as_slice());
            }
            Some(Either::Right(v)) => {
                v.encode(buf)?;
            }
            None => unreachable!(),
        };

        Ok(())
    }
}
