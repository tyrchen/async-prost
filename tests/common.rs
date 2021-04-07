use bytes::Bytes;
use futures_util::future::poll_fn;
use prost::Message;
use std::task::{Context, Poll};
use tower::Service;

pub async fn ready<S: Service<Request>, Request>(svc: &mut S) -> Result<(), S::Error> {
    poll_fn(|cx| svc.poll_ready(cx)).await
}

#[derive(Clone, PartialEq, Message)]
pub struct Event {
    #[prost(bytes = "bytes", tag = "1")]
    pub id: Bytes,
    #[prost(bytes = "bytes", tag = "2")]
    pub data: Bytes,
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

pub struct PanicError;
use std::fmt;
impl<E> From<E> for PanicError
where
    E: fmt::Debug,
{
    fn from(e: E) -> Self {
        panic!("{:?}", e)
    }
}

pub fn unwrap<T>(r: Result<T, PanicError>) -> T {
    if let Ok(t) = r {
        t
    } else {
        unreachable!();
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
