![](https://github.com/tyrchen/async-prost/workflows/build/badge.svg)

# async-prost

Async access to prost-encoded item stream, highly inspected(copied) by/from [async-bincode](https://github.com/jonhoo/async-bincode). Could be used with [tokio-tower](https://github.com/tower-rs/tokio-tower) to build TCP applications with prost encoded messages.

Usage:

```rust
#[derive(Clone, PartialEq, Message)]
pub struct Event {
    #[prost(bytes = "bytes", tag = "1")]
    pub id: Bytes,
    #[prost(bytes = "bytes", tag = "2")]
    pub data: Bytes,
}

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
```

See tests for more examples.

Have fun with this crate!

## License

This project is distributed under the terms of MIT.

See [LICENSE](LICENSE.md) for details.

Copyright 2021 Tyr Chen
