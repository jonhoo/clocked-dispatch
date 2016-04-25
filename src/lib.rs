//! Provides a message dispatch service where each receiver is aware of messages passed to other
//! peers. In particular, if a message is sent to some receiver `r`, another receiver `r'` will be
//! aware that one message has been dispatched when it does a subsequent read.
//!
//! The library ensures that a sender will not block due to the slowness of a receiver that is not
//! the intended recipient of the message in question. For example, if there are two receivers, `r`
//! and `r'`, `r.send(v)` will not block even though `r'` is not currently reading from its input
//! channel.
//!
//! # Examples:
//!
//! Simple usage:
//!
//! ```
//! use std::thread;
//! use clocked_dispatch;
//!
//! // Create a dispatcher
//! let d = clocked_dispatch::new(1);
//!
//! // Create a simple streaming channel
//! let (tx, rx) = d.new("a");
//! thread::spawn(move|| {
//!     tx.send(10);
//! });
//! assert_eq!(rx.recv().unwrap().0.unwrap(), 10);
//! ```
//!
//! Shared usage:
//!
//! ```
//! use std::thread;
//! use clocked_dispatch;
//!
//! // Create a dispatcher
//! let d = clocked_dispatch::new(1);
//!
//! // Create a shared channel that can be sent along from many threads
//! // where tx is the sending half (tx for transmission), and rx is the receiving
//! // half (rx for receiving).
//! let (tx, rx) = d.new("a");
//! for i in 0..10 {
//!     let tx = tx.clone();
//!     thread::spawn(move|| {
//!         tx.send(i);
//!     });
//! }
//!
//! for _ in 0..10 {
//!     let j = rx.recv().unwrap().0.unwrap();
//!     assert!(0 <= j && j < 10);
//! }
//! ```
//!
//! Accessing timestamps:
//!
//! ```
//! use clocked_dispatch;
//! let m = clocked_dispatch::new(1);
//! let (tx_a, rx_a) = m.new("a");
//! let (tx_b, rx_b) = m.new("b");
//!
//! tx_a.send("a1");
//! assert_eq!(rx_a.recv().unwrap(), (Some("a1"), 1));
//! assert_eq!(rx_b.recv().unwrap(), (None, 1));
//!
//! tx_a.send("a2");
//! tx_a.send("a3");
//! assert_eq!(rx_a.recv().unwrap(), (Some("a2"), 2));
//! assert_eq!(rx_a.recv().unwrap(), (Some("a3"), 3));
//! assert_eq!(rx_b.recv().unwrap(), (None, 3));
//! ```

// The library is implemented by routing all messages through a single dispatcher.
// This central dispatcher assigns a monotonically increasing timestamp to each message, and
// forwards it to the intended recipient's queue. Furthermore, the dispatcher notifies all other
// receivers about the new timestamp that has been distributed.

use std::sync::{Arc, Mutex, Condvar};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::mpsc;
use std::thread;
use std::fmt;

/// A message intended for the dispatcher.
enum Message<T> {
    Data(String, T, Option<usize>),
    Join(String, Arc<ReceiverInner<T>>),
    Leave(String),
}

// LastSender will be dropped when the last sender for a given type is dropped.
// This allows us to clean up at the dispatcher by sending it a Leave.
struct LastSender<T> {
    name: String,
    serializer: mpsc::SyncSender<Message<T>>,
}

impl<T> Drop for LastSender<T> {
    fn drop(&mut self) {
        self.serializer.send(Message::Leave(self.name.clone())).unwrap();
    }
}

/// The sending half of a clocked synchronous channel.
/// This half can only be owned by one thread, but it can be cloned to send to other threads.
///
/// Sending on a clocked channel will deliver the given message to the appropriate receiver, but
/// also notify all other receivers about the timestamp assigned to the message. The sending will
/// never block on a receiver that is not the destination of the message.
///
/// When the last ClockedSender is dropped, the dispatcher will automatically be notified.
///
/// ```
/// use clocked_dispatch;
/// use std::thread;
///
/// let m = clocked_dispatch::new(1);
/// let (tx_a, rx_a) = m.new("a");
///
/// let tx_a1 = tx_a.clone();
/// let a1 = thread::spawn(move || {
///     tx_a1.send("a1");
/// });
///
/// let tx_a2 = tx_a.clone();
/// let a2 = thread::spawn(move || {
///     tx_a2.send("a2");
/// });
///
/// drop(tx_a);
/// assert_eq!(rx_a.count(), 2);
/// ```
#[derive(Clone)]
pub struct ClockedSender<T> {
    // the mutex is only necessary to show that we don't need Sync on last.serializer
    last: Arc<Mutex<LastSender<T>>>,
    name: String,
    serializer: mpsc::SyncSender<Message<T>>,
}

impl<T> ClockedSender<T> {
    /// Sends a value on this synchronous channel, and notifies all other recipients of the
    /// timestamp it is assigned by the dispatcher.
    ///
    /// This function will *block* until space in the internal buffer becomes available, or a
    /// receiver is available to hand off the message to.
    ///
    /// Note that a successful send does *not* guarantee that the receiver will ever see the data if
    /// there is a buffer on this channel. Items may be enqueued in the internal buffer for the
    /// receiver to receive at a later time. If the buffer size is 0, however, it can be guaranteed
    /// that the receiver has indeed received the data if this function returns success.
    pub fn send(&self, data: T) {
        // XXX: would be really neat if we could return the ts here, but that'll probably be tricky
        // TODO: This function will never panic, but it may return `Err` if the `Receiver` has
        // disconnected and is no longer able to receive information.
        self.serializer.send(Message::Data(self.name.clone(), data, None)).unwrap()
    }

    /// Identical to send(), except that it asserts that the given timestamp is assigned by the
    /// dispatcher.
    pub fn send_ts(&self, data: T, ts: usize) {
        self.serializer.send(Message::Data(self.name.clone(), data, Some(ts))).unwrap()
    }
}

struct QueueState<T> {
    queue: VecDeque<(T, usize)>,
    ts_head: usize,
    ts_tail: usize,
    closed: bool,
}

struct ReceiverInner<T> {
    mx: Mutex<QueueState<T>>,
    cond: Condvar,
}

/// The receiving half of a clocked synchronous channel.
///
/// A clocked receiver will receive all messages sent by one of its associated senders. It will
/// also receive notifications whenever a message with a higher timestamp than any it has seen has
/// been sent to another receiver under the same dispatcher.
#[derive(Clone)]
pub struct ClockedReceiver<T> {
    inner: Arc<ReceiverInner<T>>,
}

impl<T> ClockedReceiver<T> {
    fn new(bound: usize) -> ClockedReceiver<T> {
        ClockedReceiver {
            inner: Arc::new(ReceiverInner {
                mx: Mutex::new(QueueState {
                    queue: VecDeque::with_capacity(bound),
                    ts_head: 0,
                    ts_tail: 0,
                    closed: false,
                }),
                cond: Condvar::new(),
            }),
        }
    }
}

impl<T> Iterator for ClockedReceiver<T> {
    type Item = (Option<T>, usize);
    fn next(&mut self) -> Option<Self::Item> {
        self.recv().ok()
    }
}

// TODO
// impl<T> Drop for ClockedReceiver<T> {
//     fn drop(&mut self) {
//         // ensure that subsequent send()'s return an error (somehow?)
//     }
// }

/// An error returned from the `recv` function on a ClockedReceiver.
///
/// The `recv` operation can only fail if the sending half of a channel is disconnected, implying
/// that no further messages will ever be received.
pub struct RecvError;

impl fmt::Debug for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "read from empty, closed channel")
    }
}

impl<T> ClockedReceiver<T> {
    /// Attempts to wait for a value on this receiver, returning an error if the corresponding
    /// channel has hung up.
    ///
    /// This function will always block the current thread if there is no data available, the
    /// receiver has seen the latest timestamp handled by the dispatcher, and it's possible for
    /// more data to be sent. Once a message is sent to the corresponding `Sender`, then this
    /// receiver will wake up and return that message. If a message is sent by another `Sender`
    /// under the same dispatcher, this receiver will wake up and receive the timestamp assigned to
    /// that message.
    ///
    /// If the corresponding `Sender` has disconnected, or it disconnects while this call is
    /// blocking, this call will wake up and return `Err` to indicate that no more messages can
    /// ever be received on this channel. However, since channels are buffered, messages sent
    /// before the disconnect will still be properly received.
    pub fn recv(&self) -> Result<(Option<T>, usize), RecvError> {
        let mut state = self.inner.mx.lock().unwrap();
        while state.ts_head == state.ts_tail && !state.closed {
            // we have observed all timestamps, so the queue must be empty
            state = self.inner.cond.wait(state).unwrap();
        }

        if state.ts_head == state.ts_tail {
            // we must be closed
            return Err(RecvError);
        }

        // if there's something at the head of the queue, return it
        if let Some((t, ts)) = state.queue.pop_front() {
            state.ts_head = ts;
            self.inner.cond.notify_one();
            return Ok((Some(t), ts));
        }

        // otherwise, notify about the newest available timestamp
        state.ts_head = state.ts_tail;
        self.inner.cond.notify_one();
        return Ok((None, state.ts_head));
    }
}

/// Dispatch coordinator for adding additional clocked channels.
pub struct Dispatcher<T> {
    serializer: mpsc::SyncSender<Message<T>>,
    bound: usize,
}

impl<T> Dispatcher<T> {
    /// Creates a new named, synchronous, bounded, clocked channel managed by this dispatcher.
    ///
    /// The given name *must* be unique for this dispatch.
    ///
    /// The `ClockedReceiver` will block until a message or a new timestamp becomes available.
    ///
    /// The receiver's incoming channel has an internal buffer on which messages will be queued.
    /// Its size is inherited from the dispatch bound. When this buffer becomes full, future
    /// messages from the dispatcher will block waiting for the buffer to open up. Note that a
    /// buffer size of 0 is valid, but its behavior differs from that of synchronous Rust channels.
    /// Because the dispatcher sits between the sender and the receiver, a bound of 0 will not
    /// guarantee a "rendezvous" between the sender and the receiver, but rather between the sender
    /// and the dispatcher (and subsequently, the dispatcher and the receiver).
    pub fn new<V: Sized + Into<String>>(&self, view: V) -> (ClockedSender<T>, ClockedReceiver<T>) {
        let name = view.into();
        let send = ClockedSender {
            last: Arc::new(Mutex::new(LastSender {
                name: name.clone(),
                serializer: self.serializer.clone(),
            })),
            name: name.clone(),
            serializer: self.serializer.clone(),
        };
        let recv = ClockedReceiver::new(self.bound);

        self.serializer.send(Message::Join(name, recv.inner.clone())).unwrap();
        (send, recv)
    }
}

/// Creates a new clocked dispatch. Dispatch channels can be constructed by calling `new` on the
/// returned dispatcher.
///
/// The dispatcher has an internal buffer for incoming messages. When this buffer becomes full,
/// future sends to the dispatcher will block waiting for the buffer to open up. Note that a buffer
/// size of 0 is valid, but its behavior differs from that of synchronous Rust channels. Because
/// the dispatcher sits between the sender and the receiver, a bound of 0 will not guarantee a
/// "rendezvous" between the sender and the receiver, but rather between the sender and the
/// dispatcher (and subsequently, the dispatcher and the receiver).
pub fn new<T: Send + 'static>(bound: usize) -> Dispatcher<T> {
    // TODO: Figure out why bound == 0 doesn't work.
    let (stx, srx) = mpsc::sync_channel(bound);

    thread::spawn(move || {
        let mut routes: HashMap<String, Arc<ReceiverInner<T>>> = HashMap::new();
        let mut ts = 1;
        for m in srx.iter() {
            match m {
                Message::Data(to, data, tagged_ts) => {
                    if let Some(ref t) = tagged_ts {
                        assert!(*t == ts, "explicit timestamp is not stricly monotonic")
                    }

                    let mts = tagged_ts.unwrap_or(ts);
                    ts = mts + 1;

                    for (rn, r) in routes.iter() {
                        if *rn == to {
                            continue;
                        }

                        let mut state = r.mx.lock().unwrap();
                        state.ts_tail = mts;
                        r.cond.notify_one();
                        drop(state);
                    }

                    let to = routes.get(&*to)
                                   .expect(&format!("tried to dispatch to unknown recipient '{}'",
                                                    to));

                    let mut state = to.mx.lock().unwrap();
                    while state.queue.len() == bound {
                        state = to.cond.wait(state).unwrap();
                    }

                    state.queue.push_back((data, mts));
                    state.ts_tail = mts;
                    to.cond.notify_one();
                    drop(state);
                }
                Message::Join(name, inner) => {
                    routes.insert(name, inner);
                }
                Message::Leave(name) => {
                    let to = routes.remove(&*name)
                                   .expect(&format!("tried to remove unknown receiver '{}'", name));
                    let mut state = to.mx.lock().unwrap();
                    state.closed = true;
                    to.cond.notify_one();
                    drop(state);
                }
            }
        }
    });

    Dispatcher {
        serializer: stx,
        bound: bound,
    }
}

#[cfg(test)]
mod tests {}
