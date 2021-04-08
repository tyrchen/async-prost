use either::Either;
use futures_util::future::poll_fn;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use prost::Message;

use async_prost::*;
use slab::Slab;
use tokio::net::{TcpListener, TcpStream};
use tokio_tower::multiplex::{Client, MultiplexTransport, Server, TagStore};
use tower::Service;

mod common;
use common::*;

pub async fn ready<S: Service<RequestFrame>, RequestFrame>(svc: &mut S) -> Result<(), S::Error> {
    poll_fn(|cx| svc.poll_ready(cx)).await
}

#[derive(Clone, PartialEq, Message)]
pub struct Header {
    #[prost(uint64, tag = "1")]
    tag: u64,
}

impl ShallDecodeBody for Header {
    fn shall_decode_body(&self) -> bool {
        self.tag % 2 == 0
    }
}
#[derive(Clone, PartialEq, Message)]
pub struct Body {
    #[prost(bytes = "bytes", tag = "1")]
    pub data: Bytes,
}

impl Body {
    pub fn new(data: Bytes) -> Self {
        Body { data }
    }
}

impl From<RequestFrame> for ResponseFrame {
    fn from(r: RequestFrame) -> ResponseFrame {
        ResponseFrame(r.0)
    }
}

#[derive(Debug, Default)]
struct RequestFrame(Frame<Header, Body>);

#[derive(Debug, Default)]
struct ResponseFrame(Frame<Header, Body>);

impl RequestFrame {
    pub fn new(data: Bytes) -> Self {
        RequestFrame(Frame {
            header: Some(Header { tag: 0 }),
            body: Some(Either::Right(Body { data })),
        })
    }

    pub fn set_tag(&mut self, tag: usize) {
        if let Some(header) = self.0.header.as_mut() {
            header.tag = tag as u64;
        }
    }
}

impl ResponseFrame {
    pub fn check_data(&self, expected: Bytes) {
        if let Either::Right(v) = self.0.body.as_ref().unwrap() {
            assert_eq!(v.data, expected);
        } else {
            assert!(false, "Should not come here")
        }
    }

    #[allow(dead_code)]
    pub fn check_body(&self, expected: Bytes) {
        if let Either::Left(v) = self.0.body.as_ref().unwrap() {
            let body = Body::new(expected);
            let mut buf: Vec<u8> = Vec::new();
            body.encode(&mut buf).unwrap();
            assert_eq!(v.as_slice(), buf.as_slice());
        } else {
            assert!(false, "Should not come here")
        }
    }
}

impl Framed for RequestFrame {
    fn decode(buf: &[u8], header_len: usize) -> Result<Self, std::io::Error> {
        let frame = Frame::decode(buf, header_len)?;
        Ok(Self(frame))
    }

    fn encoded_len(&self) -> u32
    where
        Self: Sized,
    {
        self.0.encoded_len()
    }

    fn encode<B>(&self, buf: &mut B) -> Result<(), std::io::Error>
    where
        B: bytes::BufMut,
        Self: Sized,
    {
        self.0.encode(buf)
    }
}

impl Framed for ResponseFrame {
    fn decode(buf: &[u8], header_len: usize) -> Result<Self, std::io::Error> {
        let frame = Frame::decode(buf, header_len)?;
        Ok(Self(frame))
    }

    fn encoded_len(&self) -> u32
    where
        Self: Sized,
    {
        self.0.encoded_len()
    }

    fn encode<B>(&self, buf: &mut B) -> Result<(), std::io::Error>
    where
        B: bytes::BufMut,
        Self: Sized,
    {
        self.0.encode(buf)
    }
}

impl ResponseFrame {
    pub fn get_tag(&self) -> usize {
        self.0.header.as_ref().unwrap().tag as usize
    }
}

pub struct EchoService;
impl Service<RequestFrame> for EchoService {
    type Response = ResponseFrame;
    type Error = ();
    type Future = futures_util::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, r: RequestFrame) -> Self::Future {
        futures_util::future::ok(Self::Response::from(r))
    }
}

struct SlabStore(Slab<()>);

impl TagStore<RequestFrame, ResponseFrame> for SlabStore {
    type Tag = usize;
    fn assign_tag(mut self: Pin<&mut Self>, request: &mut RequestFrame) -> usize {
        let tag = self.0.insert(());
        request.set_tag(tag);
        tag
    }
    fn finish_tag(mut self: Pin<&mut Self>, response: &ResponseFrame) -> usize {
        let tag = response.get_tag();
        self.0.remove(tag);
        tag
    }
}

#[tokio::test]
async fn framed_tokio_tower_should_work() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // connect
    let tx = TcpStream::connect(&addr).await.unwrap();
    let tx = AsyncProstStream::from(tx).for_async_framed();
    let mut tx: Client<_, PanicError, _> =
        Client::new(MultiplexTransport::new(tx, SlabStore(Slab::new())));

    // accept
    let (rx, _) = listener.accept().await.unwrap();
    let rx = AsyncProstStream::from(rx).for_async_framed();
    let server = Server::new(rx, EchoService);
    tokio::spawn(async move { server.await.unwrap() });

    unwrap(ready(&mut tx).await);

    let b1 = Bytes::from_static(b"hello");
    let b2 = Bytes::from_static(b"world");
    let b3 = Bytes::from_static(b"tyr");
    let fut1 = tx.call(RequestFrame::new(b1.clone()));
    unwrap(ready(&mut tx).await);
    let fut2 = tx.call(RequestFrame::new(b2.clone()));
    unwrap(ready(&mut tx).await);
    let fut3 = tx.call(RequestFrame::new(b3.clone()));
    unwrap(ready(&mut tx).await);

    unwrap(fut1.await).check_data(b1);
    unwrap(fut2.await).check_body(b2);
    unwrap(fut3.await).check_data(b3);
}
