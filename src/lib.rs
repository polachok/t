extern crate futures;
extern crate libc;
#[macro_use]
extern crate log;
extern crate pretty_env_logger;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::collections::VecDeque;

use libc::{fd_set, select, timeval, FD_ISSET, FD_SET, FD_ZERO};
use futures::{Future, FutureExt};
use futures::executor::{Executor, SpawnError};
use futures::io::AsyncWrite;
use futures::io::AsyncRead;
use futures::task::Context;
use futures::Async;
use futures::io::Error;
use futures::Never;
use futures::task::{Wake, Waker};
use futures::task::LocalMap;

use std::os::unix::io::RawFd;
use std::os::unix::io::AsRawFd;

use std::net::ToSocketAddrs;

use std::collections::BTreeMap;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

// reactor lives in a thread local variable. Here's where all magic happens!
thread_local! {
    static REACTOR: Rc<RefCell<InnerEventLoop>> = Rc::new(RefCell::new(InnerEventLoop::new()));
}

// The Core is just a wrapper around InnerEventLoop
// where everything happens.
#[derive(Clone)]
pub struct Core(Rc<RefCell<InnerEventLoop>>);

impl Core {
    // Create the inner event loop and save it into thread local
    pub fn new() -> Self {
        let reactor = REACTOR.with(|ev| ev.clone());

        Core(reactor)
    }

    // delegate to inner loop
    pub fn run<F: Future<Item = (), Error = ()> + 'static>(self, f: F) {
        self.0.borrow().run(f)
    }
}

// Our waker Token. It stores the index of the future in the wait queue
// (see below)
#[derive(Debug)]
struct Token(usize);

impl Wake for Token {
    fn wake(arc_self: &Arc<Token>) {
        debug!("waking {:?}", arc_self);

        let Token(idx) = **arc_self;

        // get access to the reactor by way of TLS and call wake
        REACTOR.with(|reactor| {
            let wakeup = Wakeup {
                index: idx,
                waker: Waker::from(arc_self.clone()),
            };
            reactor.borrow().wake(wakeup);
        });
    }
}

// Task is a boxed future + task-locals
struct Task {
    future: Box<Future<Item = (), Error = ()> + 'static>,
    locals: LocalMap,
}

impl Task {
    // returning Ready will lead to task being removed from wait queues and droped
    fn poll<E: Executor>(&mut self, waker: Waker, exec: &mut E) -> Async<()> {
        let mut map = &mut self.locals;
        let mut context = Context::new(&mut map, &waker, exec);

        match self.future.poll(&mut context) {
            Ok(Async::Ready(_)) => {
                debug!("future done");
                Async::Ready(())
            }
            Ok(Async::Pending) => {
                debug!("future not yet ready");
                Async::Pending
            }
            Err(_) => {
                // we don't bother about error handling
                panic!("error in future");
            }
        }
    }
}

// reactor handle, just like in real tokio
pub struct Handle(Rc<RefCell<InnerEventLoop>>);

impl Executor for Handle {
    fn spawn(
        &mut self,
        f: Box<Future<Item = (), Error = Never> + 'static + Send>,
    ) -> Result<(), SpawnError> {
        debug!("spawning from handle");
        self.0
            .borrow_mut()
            .do_spawn(f.map_err(|never| never.never_into()));
        Ok(())
    }
}

struct Wakeup {
    index: usize,
    waker: Waker,
}

// The "real" event loop.
struct InnerEventLoop {
    read: RefCell<BTreeMap<RawFd, Waker>>,
    write: RefCell<BTreeMap<RawFd, Waker>>,
    counter: RefCell<usize>,
    wait_queue: RefCell<Vec<Task>>,
    run_queue: RefCell<VecDeque<Wakeup>>,
}

impl InnerEventLoop {
    pub fn new() -> Self {
        InnerEventLoop {
            read: RefCell::new(BTreeMap::new()),
            write: RefCell::new(BTreeMap::new()),
            counter: RefCell::new(0),
            wait_queue: RefCell::new(Vec::new()),
            run_queue: RefCell::new(VecDeque::new()),
        }
    }

    pub fn handle(&self) -> Handle {
        REACTOR.with(|reactor| Handle(reactor.clone()))
    }

    // a future calls this to register its interest
    // in socket's "ready to be read" events
    fn add_read_interest(&self, fd: RawFd, waker: Waker) {
        debug!("adding read interest for {}", fd);

        if !self.read.borrow().contains_key(&fd) {
            self.read.borrow_mut().insert(fd, waker);
        }
    }

    fn remove_read_interest(&self, fd: RawFd) {
        debug!("removing read interest for {}", fd);

        self.read.borrow_mut().remove(&fd);
    }

    // see above
    fn remove_write_interest(&self, fd: RawFd) {
        debug!("removing write interest for {}", fd);

        self.write.borrow_mut().remove(&fd);
    }

    fn add_write_interest(&self, fd: RawFd, waker: Waker) {
        debug!("adding write interest for {}", fd);

        if !self.write.borrow().contains_key(&fd) {
            self.write.borrow_mut().insert(fd, waker);
        }
    }

    // waker calls this to put the future on the run queue
    fn wake(&self, wakeup: Wakeup) {
        self.run_queue.borrow_mut().push_back(wakeup);
    }

    fn next_task(&self) -> Waker {
        let w = Arc::new(Token(*self.counter.borrow()));
        *self.counter.borrow_mut() += 1;
        Waker::from(w)
    }

    // create a task, poll it once and push it on wait queue
    fn do_spawn<F: Future<Item = (), Error = ()> + 'static>(&self, f: F) {
        let map = LocalMap::new();
        let waker = self.next_task();
        let mut task = Task {
            future: Box::new(f),
            locals: map,
        };
        let mut handle = self.handle();

        {
            task.poll(waker, &mut handle)
        };

        self.wait_queue.borrow_mut().push(task)
    }

    // the meat of the event loop
    // we're using select(2) because it's simple and it's portable
    pub fn run<F: Future<Item = (), Error = ()> + 'static>(&self, f: F) {
        self.do_spawn(f);

        loop {
            debug!("select loop start");

            // event loop iteration timeout. if no descriptor
            // is ready we continue iterating
            let mut tv: timeval = timeval {
                tv_sec: 1,
                tv_usec: 0,
            };

            // initialize fd_sets (file descriptor sets)
            let mut read_fds: fd_set = unsafe { std::mem::zeroed() };
            let mut write_fds: fd_set = unsafe { std::mem::zeroed() };

            unsafe { FD_ZERO(&mut read_fds) };
            unsafe { FD_ZERO(&mut write_fds) };

            let mut nfds = 0;

            // add read interests to read fd_sets
            for fd in self.read.borrow().keys() {
                debug!("added fd {} for read", fd);
                unsafe { FD_SET(*fd, &mut read_fds as *mut fd_set) };
                nfds = std::cmp::max(nfds, fd + 1);
            }

            // add write interests to write fd_sets
            for fd in self.write.borrow().keys() {
                debug!("added fd {} for write", fd);
                unsafe { FD_SET(*fd, &mut write_fds as *mut fd_set) };
                nfds = std::cmp::max(nfds, fd + 1);
            }

            // select will block until some event happens
            // on the fds or timeout triggers
            let rv = unsafe {
                select(
                    nfds,
                    &mut read_fds,
                    &mut write_fds,
                    std::ptr::null_mut(),
                    &mut tv,
                )
            };

            // don't care for errors
            if rv == -1 {
                panic!("select()");
            } else if rv == 0 {
                debug!("timeout");
            } else {
                debug!("data available on {} fds", rv);
            }

            // check which fd it was and put appropriate future on run queue
            for (fd, waker) in self.read.borrow().iter() {
                let is_set = unsafe { FD_ISSET(*fd, &mut read_fds as *mut fd_set) };
                debug!("fd#{} set (read)", fd);
                if is_set {
                    waker.wake();
                }
            }

            // same for write
            for (fd, waker) in self.write.borrow().iter() {
                let is_set = unsafe { FD_ISSET(*fd, &mut write_fds as *mut fd_set) };
                debug!("fd#{} set (write)", fd);
                if is_set {
                    waker.wake();
                }
            }

            let mut tasks_done = Vec::new();

            // now pop futures from the run queue and poll them
            while let Some(w) = self.run_queue.borrow_mut().pop_front() {
                debug!("polling task#{}", w.index);

                let mut handle = self.handle();

                // if a task returned Ready - we're done with it
                if let Some(ref mut task) = self.wait_queue.borrow_mut().get_mut(w.index) {
                    if let Async::Ready(_) = task.poll(w.waker, &mut handle) {
                        tasks_done.push(w.index);
                    }
                }
            }

            // remove completed tasks
            for idx in tasks_done {
                info!("removing task#{}", idx);
                self.wait_queue.borrow_mut().remove(idx);
            }

            // stop the loop if no more tasks
            if self.wait_queue.borrow().is_empty() {
                return;
            }
        }
    }
}

impl Executor for InnerEventLoop {
    fn spawn(
        &mut self,
        f: Box<Future<Item = (), Error = Never> + 'static + Send>,
    ) -> Result<(), SpawnError> {
        self.do_spawn(f.map_err(|never| never.never_into()));
        Ok(())
    }
}

// AsyncTcpStream just wraps std tcp stream
#[derive(Debug)]
pub struct AsyncTcpStream(TcpStream);

impl AsyncTcpStream {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<AsyncTcpStream, std::io::Error> {
        let inner = TcpStream::connect(addr)?;

        inner.set_nonblocking(true)?;
        Ok(AsyncTcpStream(inner))
    }
}

impl Drop for AsyncTcpStream {
    fn drop(&mut self) {
        REACTOR.with(|reactor| {
            let fd = self.0.as_raw_fd();
            reactor.borrow().remove_read_interest(fd);
            reactor.borrow().remove_write_interest(fd);
        });
    }
}

impl AsyncRead for AsyncTcpStream {
    fn poll_read(&mut self, cx: &mut Context, buf: &mut [u8]) -> Result<Async<usize>, Error> {
        debug!("poll_read() called");

        let fd = self.0.as_raw_fd();
        let waker = cx.waker();

        match self.0.read(buf) {
            Ok(len) => Ok(Async::Ready(len)),
            Err(ref err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                REACTOR.with(|reactor| {
                    let reactor = reactor.borrow();
                    reactor.add_read_interest(fd, waker.clone())
                });

                Ok(Async::Pending)
            }
            Err(err) => panic!("error {:?}", err),
        }
    }
}

impl AsyncWrite for AsyncTcpStream {
    fn poll_write(&mut self, cx: &mut Context, buf: &[u8]) -> Result<Async<usize>, Error> {
        debug!("poll_write() called");

        let fd = self.0.as_raw_fd();
        let waker = cx.waker();

        match self.0.write(buf) {
            Ok(len) => Ok(Async::Ready(len)),
            Err(ref err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                REACTOR.with(|reactor| {
                    let reactor = reactor.borrow();
                    reactor.add_write_interest(fd, waker.clone())
                });

                Ok(Async::Pending)
            }
            Err(err) => panic!("error {:?}", err),
        }
    }

    fn poll_flush(&mut self, _cx: &mut Context) -> Result<Async<()>, Error> {
        debug!("poll_flush() called");
        Ok(Async::Ready(()))
    }

    fn poll_close(&mut self, _cx: &mut Context) -> Result<Async<()>, Error> {
        debug!("poll_close() called");
        Ok(Async::Ready(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        use futures::io::AsyncReadExt;
        use futures::io::AsyncWriteExt;

        pretty_env_logger::init();

        let stream = AsyncTcpStream::connect("127.0.0.1:9000").unwrap();
        println!("running");
        let data = b"hello world\n";
        let buf = vec![0; 128];
        let future = stream
            .write_all(data)
            .and_then(move |(stream, _)| {
                stream.read(buf).and_then(move |(_, buf, len)| {
                    println!("READ: {}", String::from_utf8_lossy(&buf[0..len]));
                    Ok(())
                })
            })
            .then(|_| Ok(()));
        let core = Core::new();
        core.run(future);
    }
}
