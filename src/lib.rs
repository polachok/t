#![feature(futures_api, pin, async_await, await_macro, arbitrary_self_types)]
extern crate futures;
extern crate libc;
#[macro_use]
extern crate log;
extern crate pretty_env_logger;

use std::mem::PinMut;


use futures::future::{Future, FutureObj};
use futures::task::Context;
use futures::task::{Spawn, LocalWaker, SpawnObjError};
use futures::task::{Wake, Waker};
use futures::Poll;
use libc::{fd_set, select, timeval, FD_ISSET, FD_SET, FD_ZERO};

use std::os::unix::io::RawFd;

use std::cell::{Cell, RefCell};
use std::collections::{VecDeque, BTreeMap};
use std::rc::Rc;
use std::sync::Arc;

mod async_tcp_stream;
mod async_tcp_listener;

pub use async_tcp_stream::AsyncTcpStream;
pub use async_tcp_listener::AsyncTcpListener;

// reactor lives in a thread local variable. Here's where all magic happens!
thread_local! {
    static REACTOR: Rc<EventLoop> = Rc::new(EventLoop::new());
}

type TaskId = usize;

pub fn run<F: Future<Output = ()> + Send + 'static>(f: F) {
    REACTOR.with(|reactor| reactor.run(f))
}

pub fn spawn<F: Future<Output = ()> + Send + 'static>(f: F) {
    REACTOR.with(|reactor| reactor.do_spawn(f))
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
                waker: unsafe { futures::task::local_waker(arc_self.clone()) },
            };
            reactor.wake(wakeup);
        });
    }
}

// Wakeup notification struct stores the index of the future in the wait queue
// and waker
struct Wakeup {
    index: usize,
    waker: LocalWaker,
}

// Task is a boxed future with Output = ()
struct Task {
    future: FutureObj<'static, ()>,
}

impl Task {
    // returning Ready will lead to task being removed from wait queues and dropped
    fn poll<E: Spawn>(&mut self, waker: LocalWaker, exec: &mut E) -> Poll<()> {
        let mut context = Context::new(&waker, exec);

        let future = PinMut::new(&mut self.future);

        match future.poll(&mut context) {
            Poll::Ready(_) => {
                debug!("future done");
                Poll::Ready(())
            }
            Poll::Pending => {
                debug!("future not yet ready");
                Poll::Pending
            }
        }
    }
}

// The "real" event loop.
struct EventLoop {
    read: RefCell<BTreeMap<RawFd, Waker>>,
    write: RefCell<BTreeMap<RawFd, Waker>>,
    counter: Cell<usize>,
    wait_queue: RefCell<BTreeMap<TaskId, Task>>,
    run_queue: RefCell<VecDeque<Wakeup>>,
}

impl EventLoop {
    fn new() -> Self {
        EventLoop {
            read: RefCell::new(BTreeMap::new()),
            write: RefCell::new(BTreeMap::new()),
            counter: Cell::new(0),
            wait_queue: RefCell::new(BTreeMap::new()),
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

        self.read.borrow_mut().insert(fd, waker);
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

        self.write.borrow_mut().insert(fd, waker);
    }

    // waker calls this to put the future on the run queue
    fn wake(&self, wakeup: Wakeup) {
        self.run_queue.borrow_mut().push_back(wakeup);
    }

    fn next_task(&self) -> (TaskId, LocalWaker) {
        let counter = self.counter.get();
        let w = Arc::new(Token(counter));
        self.counter.set(counter + 1);
        (counter, unsafe { futures::task::local_waker(w) })
    }

    // create a task, poll it once and push it on wait queue
    fn do_spawn<F: Future<Output = ()> + Send + 'static>(&self, f: F) {
        let (id, waker) = self.next_task();
        let f = Box::new(f);
        let mut task = Task {
            future: FutureObj::new(f),
        };
        let mut handle = self.handle();

        {
            // if the task is ready immediately, don't add it to wait_queue
            if let Poll::Ready(_) = task.poll(waker, &mut handle) {
                return;
            }
        };

        self.wait_queue.borrow_mut().insert(id, task);
    }

    // the meat of the event loop
    // we're using select(2) because it's simple and it's portable
    pub fn run<F: Future<Output = ()> + Send + 'static>(&self, f: F) {
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

            // now pop wakeup notifications from the run queue and poll associated futures
            while let Some(w) = self.run_queue.borrow_mut().pop_front() {
                debug!("polling task#{}", w.index);

                let mut handle = self.handle();

                let mut task = self.wait_queue.borrow_mut().remove(&w.index);
                if let Some(mut task) = task {
                    // if a task is not ready put it back
                    if let Poll::Pending = task.poll(w.waker, &mut handle) {
                        self.wait_queue.borrow_mut().insert(w.index, task);
                    }
                    // otherwise just drop it
                }
            }

            // stop the loop if no more tasks
            if self.wait_queue.borrow().is_empty() {
                return;
            }
        }
    }
}

// reactor handle, just like in real tokio
pub struct Handle(Rc<EventLoop>);

impl Spawn for Handle {
    fn spawn_obj(&mut self, f: FutureObj<'static, ()>) -> Result<(), SpawnObjError> {
        debug!("spawning from handle");
        self.0.do_spawn(f);
        Ok(())
    }
}

impl Spawn for EventLoop {
    fn spawn_obj(&mut self, f: FutureObj<'static, ()>) -> Result<(), SpawnObjError> {
        self.do_spawn(f);
        Ok(())
    }
}
