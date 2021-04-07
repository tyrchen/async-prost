use std::pin::Pin;

use async_prost::*;
use bytes::Bytes;
use slab::Slab;
use tokio::net::{TcpListener, TcpStream};
use tokio_tower::multiplex::{Client, MultiplexTransport, Server, TagStore};

mod common;
use common::*;
use tower::Service;

pub(crate) struct SlabStore(Slab<()>);

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
    unwrap(fut1.await).check(b1);
    unwrap(fut2.await).check(b2);
    unwrap(fut3.await).check(b3);
}
