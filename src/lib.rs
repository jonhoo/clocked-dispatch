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
//!
//! // notice that we can't use _ here even though tx_b is unused because
//! // then tx_b would be dropped, causing rx_b to be closed immediately
//! let (tx_b, rx_b) = m.new("btx1", "b");
//! let _ = tx_b;
//!
//! tx_a.send("a1");
//! let x = rx_a.recv().unwrap();
//! assert_eq!(x.0, Some("a1"));
//! assert_eq!(rx_b.recv(), Ok((None, x.1)));
//!
//! tx_a.send("a2");
//! tx_a.send("a3");
//!
//! let a1 = rx_a.recv().unwrap();
//! assert_eq!(a1.0, Some("a2"));
//!
//! let a2 = rx_a.recv().unwrap();
//! assert_eq!(a2.0, Some("a3"));
//!
//! // b must see the timestamp from either a1 or a2
//! // it could see a1 if a2 hasn't yet been delivered
//! let b = rx_b.recv().unwrap();
//! assert_eq!(b.0, None);
//! assert!(b.1 == a1.1 || b.1 == a2.1);
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
use std::collections::HashSet;
use std::collections::VecDeque;
use std::collections::BinaryHeap;
use std::sync::mpsc;
use std::thread;

struct TaggedData<T> {
    from: String,
    to: Option<String>,
    ts: Option<usize>,
    data: Option<T>,
}

/// A message intended for the dispatcher.
enum Message<T> {
    Data(TaggedData<T>),
    ReceiverJoin(String, Arc<ReceiverInner<T>>),
    ReceiverLeave(String),
    SenderJoin(Option<String>, String),
    SenderLeave(Option<String>, String),
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
/// When the last `ClockedSender` is dropped for a target, and there are no `ClockedBroadcaster`s,
/// the dispatcher will automatically be notified, and the recipient will see a disconnected
/// channel error once it has read all buffered messages.
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
    dispatcher: mpsc::SyncSender<Message<T>>,
}

impl<T> Drop for ClockedSender<T> {
    fn drop(&mut self) {
        self.dispatcher
            .send(Message::SenderLeave(Some(self.target.clone()), self.source.clone()))
            .unwrap();
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
        self.dispatcher
            .send(Message::Data(TaggedData {
                from: self.source.clone(),
                to: Some(self.target.clone()),
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
        self.dispatcher
            .send(Message::Data(TaggedData {
                from: self.source.clone(),
                to: Some(self.target.clone()),
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
        self.dispatcher
            .send(Message::SenderJoin(Some(self.target.clone()), source.clone()))
            .unwrap();
        ClockedSender {
            source: source,
            target: self.target.clone(),
            dispatcher: self.dispatcher.clone(),
        }
    }
}

impl<T: Clone> ClockedSender<T> {
    /// Converts this sender into a broadcast sender.
    ///
    /// Doing so detaches the sender from its receiver, and means all future sends will be
    /// broadcast to all receivers. Note that the existence of a broadcaster prevents the closing
    /// of all channels.
    pub fn to_broadcaster(self) -> ClockedBroadcaster<T> {
        let dispatcher = self.dispatcher.clone();
        let source = format!("{}_bcast", self.source);
        dispatcher.send(Message::SenderJoin(None, source.clone())).unwrap();

        // NOTE: the drop of self causes a Message::SenderLeave to be sent for this sender
        ClockedBroadcaster {
            source: source,
            dispatcher: dispatcher,
        }
    }
}

/// A sending half of a clocked synchronous channel that only allows broadcast. This half can only
/// be owned by one thread, but it can be cloned to send to other threads. A `ClockedBroadcaster`
/// can be constructed from a `ClockedSender` using `ClockedSender::to_broadcaster`.
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
/// Note that the existence of a `ClockedBroadcater` prevents the closing of any clocked channels
/// managed by this dispatcher.
///
/// # Examples
///
/// Regular broadcast:
///
/// ```
/// use clocked_dispatch;
/// use std::sync::mpsc;
///
/// let m = clocked_dispatch::new(10);
/// let (tx_a, rx_a) = m.new("atx", "arx");
/// let tx = tx_a.to_broadcaster();
/// // note that the A channel is still open since there now exists a broadcaster,
/// // even though all A senders have been dropped.
///
/// let (tx_b, rx_b) = m.new("btx", "brx");
///
/// tx.broadcast("1");
///
/// let x = rx_a.recv().unwrap();
/// assert_eq!(x.0, Some("1"));
/// assert_eq!(rx_b.recv(), Ok(x));
///
/// // non-broadcasts still work
/// tx_b.send("2");
/// let x = rx_b.recv().unwrap();
/// assert_eq!(x.0, Some("2"));
/// assert_eq!(rx_a.recv(), Ok((None, x.1)));
///
/// // drop broadcaster
/// drop(tx);
///
/// // A is now closed because there are no more senders
/// assert_eq!(rx_a.recv(), Err(mpsc::RecvError));
///
/// // rx_b is *not* closed because tx_b still exists
/// assert_eq!(rx_b.try_recv(), Err(mpsc::TryRecvError::Empty));
///
/// drop(tx_b);
/// // rx_b is now closed because its senders have all gone away
/// assert_eq!(rx_b.recv(), Err(mpsc::RecvError));
/// ```
///
/// Forwarding broadcast:
///
/// ```
/// use clocked_dispatch;
/// use std::sync::mpsc;
///
/// let m = clocked_dispatch::new(10);
/// let (tx_a, rx_a) = m.new("atx", "arx");
/// let (tx_b, rx_b) = m.new("btx", "brx");
/// let (tx_c, rx_c) = m.new("ctx", "crx");
///
/// let tx = tx_a.to_broadcaster();
/// tx.broadcast_forward(Some("1"), 1);
///
/// // all inputs aren't yet up-to-date to 1
/// assert_eq!(rx_a.try_recv(), Err(mpsc::TryRecvError::Empty));
///
/// // bring all inputs up to date
/// tx_b.forward(None, 1);
/// tx_c.forward(None, 1);
///
/// // now broadcast is delivered
/// assert_eq!(rx_a.recv().unwrap(), (Some("1"), 1));
/// assert_eq!(rx_b.recv().unwrap(), (Some("1"), 1));
/// assert_eq!(rx_c.recv().unwrap(), (Some("1"), 1));
///
/// // non-broadcasts still work (we still need to keep inputs up-to-date)
/// tx.broadcast_forward(None, 2);
/// tx_b.forward(None, 2);
/// tx_c.forward(Some("c"), 2);
/// assert_eq!(rx_a.recv().unwrap(), (None, 2));
/// assert_eq!(rx_b.recv().unwrap(), (None, 2));
/// assert_eq!(rx_c.recv().unwrap(), (Some("c"), 2));
/// ```
pub struct ClockedBroadcaster<T: Clone> {
    source: String,
    dispatcher: mpsc::SyncSender<Message<T>>,
}

impl<T: Clone> Drop for ClockedBroadcaster<T> {
    fn drop(&mut self) {
        self.dispatcher.send(Message::SenderLeave(None, self.source.clone())).unwrap();
    }
}

impl<T: Clone> ClockedBroadcaster<T> {
    /// Sends a value to all receivers known to this dispatcher. The value will be assigned a
    /// sequence number by the dispatcher.
    ///
    /// This function will *block* until space in the internal buffer becomes available, or a
    /// receiver is available to hand off the message to.
    ///
    /// Note that a successful send does *not* guarantee that the receiver will ever see the data if
    /// there is a buffer on this channel. Items may be enqueued in the internal buffer for the
    /// receiver to receive at a later time. If the buffer size is 0, however, it can be guaranteed
    /// that the receiver has indeed received the data if this function returns success.
    pub fn broadcast(&self, data: T) {
        self.dispatcher
            .send(Message::Data(TaggedData {
                from: self.source.clone(),
                to: None,
                ts: None,
                data: Some(data),
            }))
            .unwrap()
    }

    /// Sends an already-sequenced value to all receivers known to this dispatcher. The message may
    /// be buffered by the dispatcher until it can guarantee that no other sender will later try to
    /// send messages with a lower sequence number.
    ///
    /// This function will *block* until space in the internal buffer becomes available, or a
    /// receiver is available to hand off the message to.
    ///
    /// Note that a successful send does *not* guarantee that the receiver will ever see the data if
    /// there is a buffer on this channel. Items may be enqueued in the internal buffer for the
    /// receiver to receive at a later time. If the buffer size is 0, however, it can be guaranteed
    /// that the receiver has indeed received the data if this function returns success.
    ///
    /// It is optional to include data when forwarding. If no data is included, this message
    /// conveys to the dispatcher that this sender promises not to send later messages with a
    /// higher sequence number than the one given.
    pub fn broadcast_forward(&self, data: Option<T>, ts: usize) {
        self.dispatcher
            .send(Message::Data(TaggedData {
                from: self.source.clone(),
                to: None,
                ts: Some(ts),
                data: data,
            }))
            .unwrap()
    }

    /// Creates a new clocked broadcast sender.
    ///
    /// Clocked dispatch requires that all senders have a unique name so that the "up-to-date-ness"
    /// of the senders can be tracked reliably.
    pub fn clone<V: Into<String>>(&self, source: V) -> ClockedBroadcaster<T> {
        let source = source.into();
        self.dispatcher.send(Message::SenderJoin(None, source.clone())).unwrap();
        ClockedBroadcaster {
            source: source,
            dispatcher: self.dispatcher.clone(),
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
pub struct ClockedReceiver<T: Send + 'static> {
    dispatcher: mpsc::SyncSender<Message<T>>,
    inner: Arc<ReceiverInner<T>>,
    name: String,
}

impl<T: Send + 'static> ClockedReceiver<T> {
    fn new<V: Into<String>>(name: V,
                            dispatcher: mpsc::SyncSender<Message<T>>,
                            bound: usize)
                            -> ClockedReceiver<T> {
        ClockedReceiver {
            dispatcher: dispatcher,
            inner: Arc::new(ReceiverInner {
                mx: Mutex::new(QueueState {
                    queue: VecDeque::with_capacity(bound),
                    ts_head: 0,
                    ts_tail: 0,
                    closed: false,
                }),
                cond: Condvar::new(),
            }),
            name: name.into(),
        }
    }
}

impl<T: Send + 'static> Iterator for ClockedReceiver<T> {
    type Item = (Option<T>, usize);
    fn next(&mut self) -> Option<Self::Item> {
        self.recv().ok()
    }
}

impl<T: Send + 'static> Drop for ClockedReceiver<T> {
    fn drop(&mut self) {
        use std::mem;

        let name = mem::replace(&mut self.name, String::new());
        let dp = self.dispatcher.clone();
        thread::spawn(move || {
            dp.send(Message::ReceiverLeave(name)).unwrap();
        });

        // we're dropping the receiver, but the dispatcher might be blocked trying to send us
        // something. we therefore need to keep reading until the dispatcher actually gets around
        // to closing the channel. this can be done in a separate thread to let the drop finish
        // though, since there's no reason to make the caller wait for this to happen (I think).
        self.count(); // count will iterate on the channel until it's closed
    }
}

impl<T: Send + 'static> ClockedReceiver<T> {
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
pub struct Dispatcher<T: Send> {
    dispatcher: mpsc::SyncSender<Message<T>>,
    bound: usize,
}

impl<T: Send> Dispatcher<T> {
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
            dispatcher: self.dispatcher.clone(),
        };
        let recv = ClockedReceiver::new(target.clone(), self.dispatcher.clone(), self.bound);

        self.dispatcher.send(Message::ReceiverJoin(target.clone(), recv.inner.clone())).unwrap();
        self.dispatcher.send(Message::SenderJoin(Some(target.clone()), source)).unwrap();
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
    // the receiver's channel
    channel: Arc<ReceiverInner<T>>,
    // messages for this receiver that have high timestamps, and must be delayed
    delayed: BinaryHeap<Delayed<T>>,
    // known senders for this receiver
    senders: HashSet<String>,
}

// TODO:
// It seems like dispatchers are always used in one of the following ways:
//
//  - Multi-in, multi-out, unicast, assigning timestamps, no buffering
//  - Multi-in, single-out, unicast, forwarding, buffering
//  - Single-in, multi-out, broadcast, forwarding, no buffering
//
// We could specialize for each of these, which might increase performance and further modularize
// the code. This would also allow restricting the API such that you can't start using one kind of
// dispatcher in another mode. Another potentially good example of this is forcing T: Clone only
// for broadcast dispatchers.
struct DispatchInner<T> {
    // per-receiver information
    targets: HashMap<String, Target<T>>,
    // per-sender information
    freshness: HashMap<String, usize>,
    // essentially targets.keys()
    // kept separate so we can mutably use targets while iterating over all receiver names
    destinations: HashSet<String>,
    // known broadcasters
    broadcasters: HashSet<String>,
    // broadcast messages that have high timestamps, and must be delayed
    bdelay: BinaryHeap<Delayed<T>>,
    // whether we are operating in forwarding or serializing mode
    // in the former, the senders assign timestamps
    // in the latter, we assign the timestamps
    // the first message we receive dictate the mode
    forwarding: Option<bool>,
    // queue bound
    bound: usize,
}

impl<T: Clone> DispatchInner<T> {
    /// Notifies all receivers of the given timestamp, and sends any given data to the intended
    /// recipients.
    ///
    /// If `data == None`, `ts` is sent all receivers.
    /// If `to == None`, `data.unwrap()` is sent to all receivers.
    /// If `to == Some(t)`, `data.unwrap()` is sent to the the receiver named `t`.
    fn notify(&self, to: Option<&String>, ts: usize, data: Option<T>) {
        for (tn, t) in self.targets.iter() {
            let mut state = t.channel.mx.lock().unwrap();
            if data.is_some() {
                if to.is_none() || to.unwrap() == tn.as_str() {
                    while state.queue.len() == self.bound {
                        state = t.channel.cond.wait(state).unwrap();
                    }

                    // TODO: avoid clone() for the last send
                    state.queue.push_back((data.clone().unwrap(), ts));
                }
            }
            state.ts_tail = ts;
            t.channel.cond.notify_one();
            drop(state);
        }

        // if data.is_some() && to.is_some() && !self.targets.contains_key(to.unwrap().as_str())
        // this seems like a bad case, but it could just be that the receiver has left
        // TODO: would be nice if we had some way of notifying the sender that this is the case
    }

    /// Finds the minimum sequence number across all senders.
    fn min(&self) -> usize {
        self.freshness.values().min().and_then(|m| Some(*m)).unwrap_or(usize::max_value() - 1)
    }

    /// Should be called whenever the set of senders changes to process.
    /// Processes delayed messages that can now be delivered, and closes channels with no senders.
    fn senders_changed(&mut self, to: Option<String>) {
        // if the removed sender has been delaying delivery of messages (by virtue of being the
        // sender with the lowest sequence number), we may now be able to send some more messages.
        // this can only happen if we're in forwarding mode, because in assignment mode, senders
        // don't have a way to not be up-to-date, and so there are no delayed messages.
        if let Some(true) = self.forwarding {
            self.process_delayed(to.as_ref());
        }

        if self.broadcasters.is_empty() {
            // if there are broadcasters, no channel is closed
            for (_, t) in self.targets.iter_mut().filter(|&(_, ref t)| t.senders.is_empty()) {
                // having no senders when there are no broadcasters means the channel is closed
                let mut state = t.channel.mx.lock().unwrap();
                state.closed = true;
                t.channel.cond.notify_one();
                drop(state);
            }
        }
    }

    /// Find any delayed messages that are now earlier than the minimum sender sequence number, and
    /// send them in-order. Will check both broadcast messages and messages to a given sender if
    /// `to.is_some()`.
    fn process_delayed(&mut self, to: Option<&String>) {
        let min = self.min();

        // keep track of the largest timestamp we notified receivers about
        // so that we also know to update their up-to-date-ness if the min
        // is greater than all queued messages' sequence numbers.
        let mut forwarded = 0;

        // keep looking for a candidate to send
        loop {
            // we need to find the message in `[bdelay + targets[to].delayed]` with the lowest
            // timestamp. we do this by:
            //
            // 1. finding the smallest in `bdelay`
            let next = self.bdelay.peek().and_then(|d| Some(d.ts)).unwrap_or(min + 1);
            if let Some(to) = to {
                if self.targets.contains_key(to.as_str()) {
                    // 2. finding the smallest in `targets[to].delayed`
                    let tnext = self.targets[to.as_str()]
                                    .delayed
                                    .peek()
                                    .and_then(|d| Some(d.ts))
                                    .unwrap_or(min + 1);

                    // 3. using the message from 2 if it is the earliest (and early enough)
                    if tnext < next && tnext <= min {
                        let d = self.targets.get_mut(to.as_str()).unwrap().delayed.pop().unwrap();
                        forwarded = d.ts;
                        self.notify(Some(to), d.ts, Some(d.data));
                        continue;
                    }
                }
            }

            // 4. using the message from 1 (which must now be the earliest) if it is early enough
            if next <= min {
                let d = self.bdelay.pop().unwrap();
                forwarded = d.ts;
                self.notify(None, d.ts, Some(d.data));
                continue;
            }

            // no delayed message has a sequence number <= min
            break;
        }

        // make sure all dependents know how up-to-date we are
        // even if we didn't send a delayed message for the min
        if forwarded < min {
            self.notify(None, min, None);
        }
    }

    /// Takes a message from any sender, handles control messages, and delays or delivers data
    /// messages.
    ///
    /// The first data message sets which mode the dispatcher operates in. If the first message has
    /// a sequence number, the dispatcher will operate in forwarding mode. If it does not, it will
    /// operate in assignment mode. In the former, it expects every data message to be numbered,
    /// and delays too-new messages until all inputs are at least that up-to-date. In the latter,
    /// it will deliver all messages immediately, and will assign sequence numbers to each one.
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
                let min = self.min();
                if ts <= min || td.ts.is_none() {
                    // this update doesn't need to be delayed
                    // OR
                    // the sender leaves it up to us to pick timestamps, so we know we're always up
                    // to date. note that this latter case assumes that the senders will *never*
                    // give us timestamps once they have let us pick once.
                    // XXX: is ts < min possible?
                    self.notify(td.to.as_ref(), ts, td.data);
                } else {
                    // we need to buffer this update until the other views are
                    // sufficiently up-to-date. technically, if this increments the
                    // min, this could happen immediately, but pushing this here both
                    // avoids duplication of code, and ensures that we process the
                    // updates in order.
                    if let Some(data) = td.data {
                        if let Some(ref to) = td.to {
                            self.targets.get_mut(to).unwrap().delayed.push(Delayed {
                                ts: ts,
                                data: data,
                            });
                        } else {
                            self.bdelay.push(Delayed {
                                ts: ts,
                                data: data,
                            });
                        }
                    }

                    let old = self.freshness[&*td.from];
                    if ts > old {
                        // we increment at least one min
                        self.freshness.insert(td.from.clone(), ts);
                        if old == min {
                            // we *may* have increased the global min
                            self.process_delayed(td.to.as_ref());
                        }
                    }
                }
            }
            Message::ReceiverJoin(name, inner) => {
                self.targets.insert(name.clone(),
                                    Target {
                                        channel: inner,
                                        senders: HashSet::new(),
                                        delayed: BinaryHeap::new(),
                                    });
                self.destinations.insert(name);
            }
            Message::ReceiverLeave(name) => {
                // Close the channel (to allow the receiver cleanup to complete)
                {
                    // scope to end immutable borrow of self.targets
                    let t = &self.targets[&*name];
                    let mut state = t.channel.mx.lock().unwrap();
                    state.closed = true;
                    t.channel.cond.notify_one();
                    drop(state);
                }

                // Deregister all senders (so they don't hold up due to freshness)
                {
                    for s in self.targets[&*name].senders.iter() {
                        self.freshness.remove(s.as_str());
                    }
                }

                // Deregister the receiver
                self.targets.remove(&*name);
                self.destinations.remove(&*name);

                // TODO: ensure that subsequent send()'s return an error (somehow?) instead of just
                // crashing and burning (panic) like what happens now.
            }
            Message::SenderJoin(target, source) => {
                self.freshness.insert(source.clone(), 0);
                if let Some(target) = target {
                    self.targets.get_mut(&*target).unwrap().senders.insert(source);
                } else {
                    self.broadcasters.insert(source);
                }
            }
            Message::SenderLeave(target, source) => {
                if let Some(ref target) = target {
                    // NOTE: target may not exist because receiver has left
                    if let Some(target) = self.targets.get_mut(target.as_str()) {
                        target.senders.remove(&*source);
                    }
                } else {
                    self.broadcasters.remove(&*source);
                }

                self.freshness.remove(&*source);
                self.senders_changed(target);
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
pub fn new<T: Clone + Send + 'static>(bound: usize) -> Dispatcher<T> {
    let (stx, srx) = mpsc::sync_channel(bound);
    let mut d = DispatchInner {
        targets: HashMap::new(),
        destinations: HashSet::new(),
        bdelay: BinaryHeap::new(),
        broadcasters: HashSet::new(),
        freshness: HashMap::new(),
        forwarding: None,
        bound: bound,
    };

    thread::spawn(move || {
        for m in srx.iter() {
            d.absorb(m);
        }
    });

    Dispatcher {
        dispatcher: stx,
        bound: bound,
    }
}

/// Fuses together the output streams of multiple clocked receivers into another clocked stream.
///
/// This lets you wait for updates from many different senders, maintaining the guarantees of
/// in-order, clocked delivery. Once all receivers managed by the fuse are up to date to some
/// sequence number `x`, all messages with sequence number `<= x` will be delivered by the fuse
/// output in order of their sequence numbers.
///
/// # Examples
///
/// Simple usage:
///
/// ```
/// use clocked_dispatch;
///
/// let d = clocked_dispatch::new(10);
/// let (tx1, rx1) = d.new("tx1", "rx1");
/// let (tx2, rx2) = d.new("tx2", "rx2");
///
/// let fused = clocked_dispatch::fuse(vec![rx1, rx2], 10);
///
/// tx1.send("1");
/// let rx1 = fused.recv().unwrap();
///
/// tx2.send("2");
/// let rx2 = fused.recv().unwrap();
///
/// assert_eq!(rx1.0, Some("1"));
/// assert_eq!(rx2.0, Some("2"));
/// assert!(rx2.1 > rx1.1);
/// ```
///
/// Clocked delivery:
///
/// ```
/// use clocked_dispatch;
///
/// let d = clocked_dispatch::new(10);
/// let (tx1, rx1) = d.new("tx1", "rx1");
/// let (_, rx2) = d.new("tx2", "rx2");
/// let (tx3, _) = d.new("tx3", "rx3");
///
/// // notice that rx3 is *not* fused
/// let fused = clocked_dispatch::fuse(vec![rx1, rx2], 10);
///
/// tx3.send("3");
/// assert_eq!(fused.recv().unwrap().0, None);
///
/// tx1.send("1");
/// assert_eq!(fused.recv().unwrap().0, Some("1"));
/// ```
pub fn fuse<T: Clone + Send + 'static>(sources: Vec<ClockedReceiver<T>>,
                                       bound: usize)
                                       -> ClockedReceiver<T> {
    let dispatch = new(bound);
    let (dtx, drx) = dispatch.new("_unused_", "fuse_target");

    // create all the senders
    let mut dtxs = (0..sources.len())
                       .into_iter()
                       .map(|i| dtx.clone(sources[i].name.clone()))
                       .collect::<Vec<_>>();

    // base receiver no longer needed
    drop(dtx);

    // reverse since we pop below
    dtxs.reverse();

    for s in sources.into_iter() {
        let tx = dtxs.pop().unwrap();
        thread::spawn(move || {
            for (m, ts) in s {
                tx.forward(m, ts);
            }
        });
    }

    drx
}

#[cfg(test)]
mod tests {
    #[test]
    fn can_send_after_recv_drop() {
        // Create a dispatcher
        let d = super::new(1);

        // Create two channels
        let (tx_a, rx_a) = d.new("atx", "arx");
        let (tx_b, rx_b) = d.new("btx", "brx");
        let _ = tx_a;

        // Drop a receiver
        drop(rx_a);

        // Ensure that sending doesn't block forever
        tx_b.send(10);

        // And that messages are still delivered
        assert_eq!(rx_b.recv().unwrap().0.unwrap(), 10);
    }

    #[test]
    fn can_forward_after_recv_drop() {
        // Create a dispatcher
        let d = super::new(1);

        // Create two channels
        let (tx_a, rx_a) = d.new("atx", "arx");
        let (tx_b, rx_b) = d.new("btx", "brx");
        let _ = tx_a;

        // Drop a receiver
        drop(rx_a);

        // Ensure that forwarding doesn't block forever
        tx_b.forward(Some(10), 1);

        // And that messages are still delivered
        assert_eq!(rx_b.recv(), Ok((Some(10), 1)));
    }
}
