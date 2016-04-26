//! Provides a message dispatch service where each receiver is aware of messages passed to other
//! peers. In particular, if a message is sent to some receiver `r`, another receiver `r'` will be
//! aware that one message has been dispatched when it does a subsequent read. Furthermore, the
//! dispatcher ensures that messages are delivered in order by not emitting data until all input
//! sources have confirmed that they will not send data with lower sequence numbers.
//!
//! The library ensures that a sender will not block due to the slowness of a receiver that is not
//! the intended recipient of the message in question. For example, if there are two receivers, `r`
//! and `r'`, `r.send(v)` will not block even though `r'` is not currently reading from its input
//! channel.
//!
//! The library is implemented by routing all messages through a single dispatcher.
//! This central dispatcher operates in one of two modes, *forwarding* or *serializing*.
//!
//!  - In serializing mode, it assigns a monotonically increasing timestamp to each message, and
//!    forwards it to the intended recipient's queue.
//!  - In forwarding mode, it accepts timestamped messages from sources, and outputs them to the
//!    intended recipients *in order*. Messages are buffered by the dispatcher until each of the
//!    receiver's sources are at least as up-to-date as the message's timestamp.
//!
//! This dual-mode operation allows dispatchers to be composed in a hierarchical fashion, with a
//! serializing dispatcher at the "top", and forwarding dispatchers "below".
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
//! let (tx, rx) = d.new("atx1", "arx");
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
//! // Create a dispatcher.
//! // Notice that we need more buffer space to the dispatcher here.
//! // This is because clone() needs to talk to the dispatcher, but the buffer to the dispatcher
//! // may already have been filled up by the sends in the threads we spawned.
//! let d = clocked_dispatch::new(10);
//!
//! // Create a shared channel that can be sent along from many threads
//! // where tx is the sending half (tx for transmission), and rx is the receiving
//! // half (rx for receiving).
//! let (tx, rx) = d.new("atx", "arx");
//! for i in 0..10 {
//!     let tx = tx.clone(format!("atx{}", i));
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
//! let m = clocked_dispatch::new(10);
//! let (tx_a, rx_a) = m.new("atx1", "a");
//! let (_, rx_b) = m.new("btx1", "b");
//!
//! tx_a.send("a1");
//! let x = rx_a.recv().unwrap();
//! assert_eq!(x.0, Some("a1"));
//! assert_eq!(rx_b.recv().unwrap(), (None, x.1));
//!
//! tx_a.send("a2");
//! tx_a.send("a3");
//!
//! assert_eq!(rx_a.recv().unwrap().0, Some("a2"));
//!
//! let x = rx_a.recv().unwrap();
//! assert_eq!(x.0, Some("a3"));
//! assert_eq!(rx_b.recv().unwrap(), (None, x.1));
//! ```
//!
//! In-order delivery
//!
//! ```
//! use clocked_dispatch;
//! use std::sync::mpsc;
//!
//! let m = clocked_dispatch::new(10);
//! let (tx1, rx) = m.new("tx1", "a");
//! let tx2 = tx1.clone("tx2");
//!
//! tx1.forward(Some("a1"), 1);
//! assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Empty));
//!
//! tx2.forward(None, 1);
//! assert_eq!(rx.recv(), Ok((Some("a1"), 1)));
//! ```

extern crate time;

use std::sync::{Arc, Mutex, Condvar};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::BinaryHeap;
use std::sync::mpsc;
use std::thread;

struct TaggedData<T> {
    from: String,
    to: String,
    ts: Option<usize>,
    data: Option<T>,
}

/// A message intended for the dispatcher.
enum Message<T> {
    Data(TaggedData<T>),
    NewReceiver(String, Arc<ReceiverInner<T>>),
    NewSender(String, String),
    Leave(String, String),
}

/// The sending half of a clocked synchronous channel.
/// This half can only be owned by one thread, but it can be cloned to send to other threads.
///
/// Sending on a clocked channel will deliver the given message to the appropriate receiver, but
/// also notify all other receivers about the timestamp assigned to the message. The sending will
/// never block on a receiver that is not the destination of the message.
///
/// Beware that dropping a clocked sender incurs control messages to the dispatcher, and that those
/// control messages may result in messages being sent to receivers. If the dispatch channel is not
/// sufficiently buffered, this means that dropping a `ClockedSender` before the corresponding
/// `ClockedReceiver` is receiving on its end of the channel may deadlock.
///
/// When the last ClockedSender is dropped, the dispatcher will automatically be notified, and the
/// recipient will see a disconnected channel error once it has read all buffered messages.
///
/// ```
/// use clocked_dispatch;
/// use std::thread;
///
/// let m = clocked_dispatch::new(10);
/// let (tx_a, rx_a) = m.new("atx", "arx");
///
/// let tx_a1 = tx_a.clone("atx1");
/// thread::spawn(move || {
///     tx_a1.send("a1");
/// });
///
/// let tx_a2 = tx_a.clone("atx2");
/// thread::spawn(move || {
///     tx_a2.send("a2");
/// });
///
/// drop(tx_a);
/// assert_eq!(rx_a.count(), 2);
/// ```
pub struct ClockedSender<T> {
    target: String,
    source: String,
    serializer: mpsc::SyncSender<Message<T>>,
}

impl<T> Drop for ClockedSender<T> {
    fn drop(&mut self) {
        self.serializer.send(Message::Leave(self.target.clone(), self.source.clone())).unwrap();
    }
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
        self.serializer
            .send(Message::Data(TaggedData {
                from: self.source.clone(),
                to: self.target.clone(),
                ts: None,
                data: Some(data),
            }))
            .unwrap()
    }

    /// Sends an already-sequenced value to the associated receiver. The message may be buffered
    /// by the dispatcher until it can guarantee that no other sender will later try to send
    /// messages with a lower sequence number.
    ///
    /// It is optional to include data when forwarding. If no data is included, this message
    /// conveys to the dispatcher that this sender promises not to send later messages with a
    /// higher sequence number than the one given.
    pub fn forward(&self, data: Option<T>, ts: usize) {
        self.serializer
            .send(Message::Data(TaggedData {
                from: self.source.clone(),
                to: self.target.clone(),
                ts: Some(ts),
                data: data,
            }))
            .unwrap()
    }

    /// Creates a new clocked sender for this sender's receiver.
    ///
    /// Clocked dispatch requires that all senders have a unique name so that the "up-to-date-ness"
    /// of the senders can be tracked reliably.
    pub fn clone<V: Into<String>>(&self, source: V) -> ClockedSender<T> {
        let source = source.into();
        self.serializer.send(Message::NewSender(self.target.clone(), source.clone())).unwrap();
        ClockedSender {
            source: source,
            target: self.target.clone(),
            serializer: self.serializer.clone(),
        }
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

impl<T> ClockedReceiver<T> {
    /// Attempts to wait for a value on this receiver, returning an error if the corresponding
    /// channel has hung up.
    ///
    /// This function will always block the current thread if there is no data available, the
    /// receiver has seen the latest timestamp handled by the dispatcher, and it's possible for
    /// more data to be sent. Once a message is sent to a corresponding `ClockedSender`, then this
    /// receiver will wake up and return that message. If a message is sent by a `ClockedSender`
    /// connected to a different receiver under the same dispatcher, this receiver will wake up and
    /// receive the timestamp assigned to that message.
    ///
    /// If all corresponding `ClockedSender` have disconnected, or disconnect while this call is
    /// blocking, this call will wake up and return `Err` to indicate that no more messages can
    /// ever be received on this channel. However, since channels are buffered, messages sent
    /// before the disconnect will still be properly received.
    pub fn recv(&self) -> Result<(Option<T>, usize), mpsc::RecvError> {
        let mut state = self.inner.mx.lock().unwrap();
        while state.ts_head == state.ts_tail && !state.closed {
            // we have observed all timestamps, so the queue must be empty
            state = self.inner.cond.wait(state).unwrap();
        }

        if state.ts_head == state.ts_tail {
            // we must be closed
            return Err(mpsc::RecvError);
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

    /// Attempts to return a pending value on this receiver without blocking
    ///
    /// This method will never block the caller in order to wait for data to become available.
    /// Instead, this will always return immediately with a possible option of pending data on the
    /// channel.
    ///
    /// This is useful for a flavor of "optimistic check" before deciding to block on a receiver.
    pub fn try_recv(&self) -> Result<(Option<T>, usize), mpsc::TryRecvError> {
        let mut state = self.inner.mx.lock().unwrap();
        if state.ts_head == state.ts_tail && !state.closed {
            // we have observed all timestamps, so the queue must be empty
            return Err(mpsc::TryRecvError::Empty);
        }

        if state.ts_head == state.ts_tail {
            // we must be closed
            return Err(mpsc::TryRecvError::Disconnected);
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
    /// The given receiver and sender names *must* be unique for this dispatch.
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
    pub fn new<V: Sized + Into<String>>(&self,
                                        sender: V,
                                        receiver: V)
                                        -> (ClockedSender<T>, ClockedReceiver<T>) {
        let source = sender.into();
        let target = receiver.into();
        let send = ClockedSender {
            source: source.clone(),
            target: target.clone(),
            serializer: self.serializer.clone(),
        };
        let recv = ClockedReceiver::new(self.bound);

        self.serializer.send(Message::NewReceiver(target.clone(), recv.inner.clone())).unwrap();
        self.serializer.send(Message::NewSender(target.clone(), source)).unwrap();
        (send, recv)
    }
}

/// `Delayed` is used to keep track of messages that cannot yet be safely delivered because it
/// would violate the in-order guarantees.
///
/// `Delayed` structs are ordered by their timestamp such that the *lowest* is the "highest". This
/// is so that `Delayed` can easily be used in a `BinaryHeap`.
struct Delayed<T> {
    ts: usize,
    data: T,
}

impl<T> PartialEq for Delayed<T> {
    fn eq(&self, other: &Delayed<T>) -> bool {
        other.ts == self.ts
    }
}

impl<T> PartialOrd for Delayed<T> {
    fn partial_cmp(&self, other: &Delayed<T>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Eq for Delayed<T> {}

impl<T> Ord for Delayed<T> {
    fn cmp(&self, other: &Delayed<T>) -> Ordering {
        other.ts.cmp(&self.ts)
    }
}

struct Target<T> {
    channel: Arc<ReceiverInner<T>>,
    freshness: HashMap<String, usize>,
    delayed: BinaryHeap<Delayed<T>>,
}

struct DispatchInner<T> {
    targets: HashMap<String, Target<T>>,
    forwarding: Option<bool>,
    bound: usize,
}

impl<T> DispatchInner<T> {
    fn notify(&self, to: &str, ts: usize, data: Option<T>) {
        for (tn, t) in self.targets.iter() {
            if data.is_some() && tn.as_str() == to {
                continue;
            }

            let mut state = t.channel.mx.lock().unwrap();
            state.ts_tail = ts;
            t.channel.cond.notify_one();
            drop(state);
        }

        if data.is_some() {
            let to = self.targets
                         .get(to)
                         .expect(&format!("tried to dispatch to unknown recipient '{}'", to));

            let mut state = to.channel.mx.lock().unwrap();
            while state.queue.len() == self.bound {
                state = to.channel.cond.wait(state).unwrap();
            }

            state.queue.push_back((data.unwrap(), ts));
            state.ts_tail = ts;
            to.channel.cond.notify_one();
            drop(state);
        }
    }

    fn process_delayed(&mut self, to: &str) {
        let min = *self.targets[to].freshness.values().min().unwrap();
        let mut forwarded = 0;
        while self.targets[to].delayed.peek().into_iter().any(|d| d.ts <= min) {
            let d = self.targets.get_mut(to).unwrap().delayed.pop().unwrap();
            forwarded = d.ts;
            self.notify(to, d.ts, Some(d.data));
        }

        if forwarded < min {
            // make sure all dependents know we're at least this up to date
            self.notify(to, min, None);
        }
    }

    fn absorb(&mut self, m: Message<T>) {
        match m {
            Message::Data(td) => {
                if self.forwarding.is_some() {
                    assert!(self.forwarding.unwrap() == td.ts.is_some(),
                            "one sender sent timestamp, another did not");
                } else {
                    self.forwarding = Some(td.ts.is_some())
                }

                let ts = td.ts.unwrap_or(time::precise_time_ns() as usize);
                let min = *self.targets[&*td.to].freshness.values().min().unwrap();
                if ts <= min || td.ts.is_none() {
                    // this update doesn't need to be delayed
                    // OR
                    // the sender leaves it up to us to pick timestamps, so we know we're always up
                    // to date. note that this latter case assumes that the senders will *never*
                    // give us timestamps once they have let us pick once.
                    // XXX: is ts < min possible?
                    self.notify(&*td.to, ts, td.data);
                } else {
                    // we need to buffer this update until the other views are
                    // sufficiently up-to-date. technically, if this increments the
                    // min, this could happen immediately, but pushing this here both
                    // avoids duplication of code, and ensures that we process the
                    // updates in order.
                    if let Some(data) = td.data {
                        self.targets.get_mut(&*td.to).unwrap().delayed.push(Delayed {
                            ts: ts,
                            data: data,
                        });
                    }

                    let old = self.targets[&*td.to].freshness[&*td.from];
                    if ts > old {
                        // we increment at least one min
                        self.targets
                            .get_mut(&*td.to)
                            .unwrap()
                            .freshness
                            .insert(td.from.clone(), ts);
                        if old == min {
                            // we *may* have increased the global min
                            self.process_delayed(&*td.to);
                        }
                    }
                }
            }
            Message::NewReceiver(name, inner) => {
                self.targets.insert(name,
                                    Target {
                                        channel: inner,
                                        freshness: HashMap::new(),
                                        delayed: BinaryHeap::new(),
                                    });
            }
            Message::NewSender(target, source) => {
                let t = self.targets.get_mut(&*target).unwrap();
                t.freshness.insert(source, 0);
            }
            Message::Leave(target, source) => {
                self.targets
                    .get_mut(&*target)
                    .expect(&format!("tried to remove unknown receiver '{}'", target))
                    .freshness
                    .remove(&*source);

                if let Some(true) = self.forwarding {
                    // the removed sender may have been what has delayed some things
                    self.process_delayed(&*target);
                }

                if self.targets[&*target].freshness.is_empty() {
                    let mut state = self.targets[&*target].channel.mx.lock().unwrap();
                    state.closed = true;
                    self.targets[&*target].channel.cond.notify_one();
                    drop(state);
                }
            }
        }
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
///
/// Be aware that a bound of 0 means that it is not safe to drop a `ClockedSender` before the
/// corresponding `ClockedReceiver` is reading from its end of the channel.
pub fn new<T: Send + 'static>(bound: usize) -> Dispatcher<T> {
    let (stx, srx) = mpsc::sync_channel(bound);
    let mut d = DispatchInner {
        targets: HashMap::new(),
        forwarding: None,
        bound: bound,
    };

    thread::spawn(move || {
        for m in srx.iter() {
            d.absorb(m);
        }
    });

    Dispatcher {
        serializer: stx,
        bound: bound,
    }
}

#[cfg(test)]
mod tests {}
