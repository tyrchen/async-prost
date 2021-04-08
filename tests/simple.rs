use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use prost::Message;

use async_prost::*;
use futures_util::future::poll_fn;
use slab::Slab;
use tokio::net::{TcpListener, TcpStream};
use tokio_tower::multiplex::{Client, MultiplexTransport, Server, TagStore};
use tower::Service;

mod common;
use common::*;

pub async fn ready<S: Service<Request>, Request>(svc: &mut S) -> Result<(), S::Error> {
    poll_fn(|cx| svc.poll_ready(cx)).await
}

#[derive(Clone, PartialEq, Message)]
pub struct Request {
    #[prost(uint64, tag = "1")]
    tag: u64,
    #[prost(bytes = "bytes", tag = "2")]
    pub data: Bytes,
}

impl Request {
    pub fn new(data: Bytes) -> Self {
        Request { tag: 0, data }
    }

    #[allow(dead_code)]
    pub fn check(&self, expected: Bytes) {
        assert_eq!(self.data, expected);
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct Response {
    #[prost(uint64, tag = "1")]
    tag: u64,
    #[prost(bytes = "bytes", tag = "2")]
    pub data: Bytes,
}

impl From<Request> for Response {
    fn from(r: Request) -> Response {
        Response {
            tag: r.tag,
            data: r.data,
        }
    }
}

impl Response {
    pub fn check(&self, expected: Bytes) {
        assert_eq!(self.data, expected);
    }

    pub fn get_tag(&self) -> usize {
        self.tag as usize
    }
}

impl Request {
    pub fn set_tag(&mut self, tag: usize) {
        self.tag = tag as u64;
    }
}

pub struct EchoService;
impl Service<Request> for EchoService {
    type Response = Response;
    type Error = ();
    type Future = futures_util::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, r: Request) -> Self::Future {
        futures_util::future::ok(Response::from(r))
    }
}

struct SlabStore(Slab<()>);

impl TagStore<Request, Response> for SlabStore {
    type Tag = usize;
    fn assign_tag(mut self: Pin<&mut Self>, request: &mut Request) -> usize {
        let tag = self.0.insert(());
        request.set_tag(tag);
        tag
    }
    fn finish_tag(mut self: Pin<&mut Self>, response: &Response) -> usize {
        let tag = response.get_tag();
        self.0.remove(tag);
        tag
    }
}

#[tokio::test]
async fn tokio_tower_should_work() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // connect
    let tx = TcpStream::connect(&addr).await.unwrap();
    let tx = AsyncProstStream::from(tx).for_async();
    let mut tx: Client<_, PanicError, _> =
        Client::new(MultiplexTransport::new(tx, SlabStore(Slab::new())));

    // accept
    let (rx, _) = listener.accept().await.unwrap();
    let rx = AsyncProstStream::from(rx).for_async();
    let server = Server::new(rx, EchoService);
    tokio::spawn(async move { server.await.unwrap() });

    unwrap(ready(&mut tx).await);
    let b1 = Bytes::from_static(b"hello");
    let b2 = Bytes::from_static(b"world");
    let b3 = Bytes::from_static(b"tyr");
    let fut1 = tx.call(Request::new(b1.clone()));
    unwrap(ready(&mut tx).await);
    let fut2 = tx.call(Request::new(b2.clone()));
    unwrap(ready(&mut tx).await);
    let fut3 = tx.call(Request::new(b3.clone()));
    unwrap(ready(&mut tx).await);

    unwrap(fut1.await).check(b1);
    unwrap(fut2.await).check(b2);
    unwrap(fut3.await).check(b3);
}
