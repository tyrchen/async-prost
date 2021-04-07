//! Async access to a prost-encoded item stream.
//!
//! Highly inspired by [async-bincode](https://docs.rs/async-bincode).

#![deny(missing_docs)]

mod reader;
mod stream;
mod writer;

pub use crate::reader::AsyncProstReader;
pub use crate::stream::AsyncProstStream;
pub use crate::writer::{AsyncDestination, AsyncProstWriter, ProstWriterFor, SyncDestination};

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::prelude::*;
    use prost::Message;
    use tokio::{
        io::AsyncWriteExt,
        net::{TcpListener, TcpStream},
    };

    #[derive(Clone, PartialEq, Message)]
    pub struct Event {
        #[prost(bytes = "bytes", tag = "1")]
        pub id: Bytes,
        #[prost(bytes = "bytes", tag = "2")]
        pub data: Bytes,
    }

    #[tokio::test]
    async fn echo_message_should() {
        let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = echo.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = echo.accept().await.unwrap();
            let mut stream = AsyncProstStream::<_, Event, Event, _>::from(stream).for_async();
            let (r, w) = stream.tcp_split();
            r.forward(w).await.unwrap();
        });

        let stream = TcpStream::connect(&addr).await.unwrap();
        let mut client = AsyncProstStream::<_, Event, Event, _>::from(stream).for_async();
        let event = Event {
            id: Bytes::from_static(b"1234"),
            data: Bytes::from_static(b"hello world"),
        };
        client.send(event.clone()).await.unwrap();
        assert_eq!(client.next().await.unwrap().unwrap(), event);

        let event = Event {
            id: Bytes::from_static(b"1235"),
            data: Bytes::from_static(b"goodbye world"),
        };
        client.send(event.clone()).await.unwrap();
        assert_eq!(client.next().await.unwrap().unwrap(), event);
        drop(client);
    }

    #[tokio::test]
    async fn echo_lots_of_messages_should_work() {
        let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = echo.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = echo.accept().await.unwrap();
            let mut stream = AsyncProstStream::<_, Event, Event, _>::from(stream).for_async();
            let (r, w) = stream.tcp_split();
            r.forward(w).await.unwrap();
        });

        let n = 81920usize;
        let stream = TcpStream::connect(&addr).await.unwrap();
        let mut client = AsyncProstStream::<_, Event, Event, _>::from(stream).for_async();
        futures::stream::iter(0..n)
            .map(|i| {
                Ok(Event {
                    id: Bytes::from(i.to_string()),
                    data: Bytes::from_static(b"goodbye world"),
                })
            })
            .forward(&mut client)
            .await
            .unwrap();

        let stream = client.get_mut();
        stream.flush().await.unwrap();

        let mut at = 0usize;
        while let Some(got) = client.next().await.transpose().unwrap() {
            assert_eq!(Bytes::from(at.to_string()), got.id);
            at += 1;
        }
    }
}
