use log::debug;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use futures_task::{ArcWake, FutureObj};

use mio::event::Source;
use mio::{Events, Interest, Poll as MioPoll, Token};

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;
use std::sync::Arc;

mod async_tcp_listener;
mod async_tcp_stream;

pub use crate::async_tcp_listener::AsyncTcpListener;
pub use crate::async_tcp_stream::AsyncTcpStream;

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
struct MyToken(Token);

impl ArcWake for MyToken {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        debug!("waking {:?}", arc_self);

        let MyToken(idx) = **arc_self;

        // get access to the reactor by way of TLS and call wake
        REACTOR.with(|reactor| {
            let wakeup = Wakeup {
                index: idx.0,
                waker: futures_task::waker(arc_self.clone()),
            };
            reactor.wake(wakeup);
        });
    }
}

// Wakeup notification struct stores the index of the future in the wait queue
// and waker
struct Wakeup {
    index: usize,
    waker: Waker,
}

// Task is a boxed future with Output = ()
struct Task {
    future: FutureObj<'static, ()>,
}

impl Task {
    // returning Ready will lead to task being removed from wait queues and dropped
    fn poll(&mut self, waker: Waker) -> Poll<()> {
        let future = Pin::new(&mut self.future);
        let mut ctx = Context::from_waker(&waker);

        match future.poll(&mut ctx) {
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
    read: RefCell<BTreeMap<usize, Waker>>,
    write: RefCell<BTreeMap<usize, Waker>>,
    counter: Cell<usize>,
    counter2: Cell<usize>,
    wait_queue: RefCell<BTreeMap<TaskId, Task>>,
    run_queue: RefCell<VecDeque<Wakeup>>,
    poll: MioPoll,
}

impl EventLoop {
    fn new() -> Self {
        EventLoop {
            read: RefCell::new(BTreeMap::new()),
            write: RefCell::new(BTreeMap::new()),
            counter: Cell::new(0),
            counter2: Cell::new(0),
            wait_queue: RefCell::new(BTreeMap::new()),
            run_queue: RefCell::new(VecDeque::new()),
            poll: MioPoll::new().unwrap(),
        }
    }

    // a future calls this to register its interest
    // in socket's "ready to be read" events
    fn add_read_interest(&self, token: Token, source: &mut dyn Source, waker: Waker) {
        if !self.read.borrow().contains_key(&token.0) {
            if !self.write.borrow().contains_key(&token.0) {
                self.poll
                    .registry()
                    .register(source, token, Interest::READABLE)
                    .unwrap();
            } else {
                self.poll
                    .registry()
                    .reregister(source, token, Interest::WRITABLE | Interest::READABLE)
                    .unwrap();
            }

            self.read.borrow_mut().insert(token.0, waker);
        }
    }

    fn remove_read_interest(&self, token: Token, source: &mut dyn Source) {
        debug!("removing read interest for {}", token.0);

        if self.read.borrow_mut().remove(&token.0).is_some() {
            if self.write.borrow().contains_key(&token.0) {
                self.poll
                    .registry()
                    .reregister(source, token, Interest::WRITABLE)
                    .unwrap();
            } else {
                self.poll.registry().deregister(source).unwrap();
            }
        }
    }

    // see above
    fn remove_write_interest(&self, token: Token, source: &mut dyn Source) {
        debug!("removing write interest for {}", token.0);

        if self.write.borrow_mut().remove(&token.0).is_some() {
            if self.read.borrow().contains_key(&token.0) {
                self.poll
                    .registry()
                    .reregister(source, token, Interest::READABLE)
                    .unwrap();
            } else {
                self.poll.registry().deregister(source).unwrap();
            }
        }
    }

    fn add_write_interest(&self, token: Token, source: &mut dyn Source, waker: Waker) {
        debug!("adding write interest for {}", token.0);

        if !self.write.borrow().contains_key(&token.0) {
            if !self.read.borrow().contains_key(&token.0) {
                self.poll
                    .registry()
                    .register(source, token, Interest::WRITABLE)
                    .unwrap();
            } else {
                self.poll
                    .registry()
                    .reregister(source, token, Interest::WRITABLE | Interest::READABLE)
                    .unwrap();
            }

            self.write.borrow_mut().insert(token.0, waker);
        }
    }

    // waker calls this to put the future on the run queue
    fn wake(&self, wakeup: Wakeup) {
        self.run_queue.borrow_mut().push_back(wakeup);
    }

    fn next_counter2(&self) -> Token {
        let counter2 = self.counter2.get();
        self.counter2.set(counter2 + 1);
        Token(counter2)
    }

    fn next_task(&self) -> (TaskId, Waker) {
        let counter = self.counter.get();
        let w = Arc::new(MyToken(Token(counter)));
        self.counter.set(counter + 1);
        (counter, futures_task::waker(w))
    }

    // create a task, poll it once and push it on wait queue
    fn do_spawn<F: Future<Output = ()> + Send + 'static>(&self, f: F) {
        let (id, waker) = self.next_task();
        let f = Box::new(f);
        let mut task = Task {
            future: FutureObj::new(f),
        };

        // if the task is ready immediately, don't add it to wait_queue
        if let Poll::Ready(_) = task.poll(waker) {
            return;
        }

        self.wait_queue.borrow_mut().insert(id, task);
    }

    // the meat of the event loop
    pub fn run<F: Future<Output = ()> + Send + 'static>(&self, f: F) {
        self.do_spawn(f);

        // Create storage for events.
        let mut events = Events::with_capacity(128);
        loop {
            unsafe {
                let p: *const EventLoop = self;
                let q: *mut EventLoop = p as *mut EventLoop;
                (*q).poll.poll(&mut events, None).unwrap();
            }

            // check which fd it was and put appropriate future on run queue
            for event in &events {
                let t = event.token();
                if let Some(waker) = self.read.borrow().get(&t.0) {
                    waker.wake_by_ref();
                }
                if let Some(waker) = self.write.borrow().get(&t.0) {
                    waker.wake_by_ref();
                }
            }

            // now pop wakeup notifications from the run queue and poll associated futures
            loop {
                let w = self.run_queue.borrow_mut().pop_front();
                match w {
                    Some(w) => {
                        debug!("polling task#{}", w.index);

                        let task = self.wait_queue.borrow_mut().remove(&w.index);
                        if let Some(mut task) = task {
                            // if a task is not ready put it back
                            if let Poll::Pending = task.poll(w.waker) {
                                self.wait_queue.borrow_mut().insert(w.index, task);
                            }
                            // otherwise just drop it
                        }
                    }
                    None => break,
                }
            }

            // stop the loop if no more tasks
            if self.wait_queue.borrow().is_empty() {
                return;
            }
        }
    }
}
