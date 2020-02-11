use std::io;
use std::net::ToSocketAddrs;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use mio::net::TcpListener;
use mio::Token;

use futures_core::Stream;

use crate::AsyncTcpStream;
use crate::REACTOR;

use log::debug;

// AsyncTcpListener just wraps std tcp listener
#[derive(Debug)]
pub struct AsyncTcpListener {
    token: Token,
    listener: TcpListener,
}

impl AsyncTcpListener {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> Result<AsyncTcpListener, io::Error> {
        let addr = addr.to_socket_addrs()?.next().unwrap();
        let inner = TcpListener::bind(addr)?;

        REACTOR.with(|reactor| {
            let token = reactor.next_counter2();
            Ok(AsyncTcpListener {
                token: token,
                listener: inner,
            })
        })
    }

    pub fn incoming(self) -> Incoming {
        Incoming(self)
    }
}

impl Drop for AsyncTcpListener {
    fn drop(&mut self) {
        REACTOR.with(|reactor| {
            reactor.remove_read_interest(self.token, &mut self.listener);
        });
    }
}

pub struct Incoming(AsyncTcpListener);

impl Stream for Incoming {
    type Item = AsyncTcpStream;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        debug!("poll_next() called");

        let waker = ctx.waker();

        match self.0.listener.accept() {
            Ok((conn, _)) => {
                let stream = AsyncTcpStream::from_std(conn).unwrap();
                Poll::Ready(Some(stream))
            }
            Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => {
                REACTOR.with(|reactor| {
                    reactor.add_read_interest(self.0.token, &mut self.0.listener, waker.clone())
                });

                Poll::Pending
            }
            Err(err) => panic!("error {:?}", err),
        }
    }
}
