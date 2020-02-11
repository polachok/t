use mio::Token;
use std::io::Error;
use std::io::{self, Read, Write};

use std::net::ToSocketAddrs;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use mio::net::TcpStream;

use futures_io::{AsyncRead, AsyncWrite};

use log::debug;

use crate::REACTOR;

// AsyncTcpStream just wraps std tcp stream
#[derive(Debug)]
pub struct AsyncTcpStream {
    stream: TcpStream,
    token: Token,
}

impl AsyncTcpStream {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<AsyncTcpStream, io::Error> {
        let addr = addr.to_socket_addrs()?.next().unwrap();
        REACTOR.with(|reactor| {
            Ok(AsyncTcpStream {
                stream: TcpStream::connect(addr)?,
                token: reactor.next_counter2(),
            })
        })
    }

    pub fn from_std(stream: TcpStream) -> Result<AsyncTcpStream, io::Error> {
        REACTOR.with(|reactor| {
            Ok(AsyncTcpStream {
                stream: stream,
                token: reactor.next_counter2(),
            })
        })
    }
}

impl Drop for AsyncTcpStream {
    fn drop(&mut self) {
        REACTOR.with(|reactor| {
            reactor.remove_read_interest(self.token, &mut self.stream);
            reactor.remove_write_interest(self.token, &mut self.stream);
        });
    }
}

impl AsyncRead for AsyncTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        ctx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Error>> {
        debug!("poll_read() called");

        let waker = ctx.waker();

        match self.stream.read(buf) {
            Ok(len) => Poll::Ready(Ok(len)),
            Err(ref err)
                if err.kind() == io::ErrorKind::WouldBlock
                    || err.kind() == io::ErrorKind::NotConnected =>
            {
                REACTOR.with(|reactor| {
                    reactor.add_read_interest(self.token, &mut self.stream, waker.clone())
                });

                Poll::Pending
            }
            Err(err) => panic!("error {:?}", err),
        }
    }
}

impl AsyncWrite for AsyncTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        ctx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        debug!("poll_write() called");

        let waker = ctx.waker();

        match self.stream.write(buf) {
            Ok(len) => Poll::Ready(Ok(len)),
            Err(ref err)
                if err.kind() == io::ErrorKind::WouldBlock
                    || err.kind() == io::ErrorKind::NotConnected =>
            {
                REACTOR.with(|reactor| {
                    reactor.add_write_interest(self.token, &mut self.stream, waker.clone())
                });

                Poll::Pending
            }
            Err(err) => panic!("error {:?}", err),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _lw: &mut Context) -> Poll<Result<(), Error>> {
        debug!("poll_flush() called");
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _lw: &mut Context) -> Poll<Result<(), Error>> {
        debug!("poll_close() called");
        Poll::Ready(Ok(()))
    }
}
