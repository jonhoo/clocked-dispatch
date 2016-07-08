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

extern crate rand;

use std::sync::{Arc, Mutex, Condvar};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::collections::BinaryHeap;
use std::sync::mpsc;
use std::thread;
use std::sync;

macro_rules! debug {
    ( $fmt:expr ) => {
        // println!($fmt);
    };
    ( $fmt:expr, $( $args:expr ),+ ) => {
        // println!($fmt, $($args),*);
        $(let _ = $args)*;
    };
}

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
    left: bool,
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
///
/// Dropping it will unblock any senders trying to send to this receiver.
pub struct ClockedReceiver<T: Send + 'static> {
    leave: mpsc::SyncSender<String>,
    inner: Arc<ReceiverInner<T>>,
    name: String,
}

impl<T: Send + 'static> ClockedReceiver<T> {
    fn new<V: Into<String>>(name: V,
                            leave: mpsc::SyncSender<String>,
                            bound: usize)
                            -> ClockedReceiver<T> {
        ClockedReceiver {
            leave: leave,
            inner: Arc::new(ReceiverInner {
                mx: Mutex::new(QueueState {
                    queue: VecDeque::with_capacity(bound),
                    ts_head: 0,
                    ts_tail: 0,
                    closed: false,
                    left: false,
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
        self.leave.send(name).unwrap();
        // wait until we've actually been dropped
        self.count();
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
        while state.ts_head == state.ts_tail && state.queue.is_empty() && !state.closed {
            // NOTE: is is *not* sufficient to use head == tail as an indicator that there are no
            // messages. specifically, if there are duplicates for a given timestamp, the equality
            // may work out while there are still elements in the queue.
            state = self.inner.cond.wait(state).unwrap();
        }

        // if there's something at the head of the queue, return it
        if let Some((t, ts)) = state.queue.pop_front() {
            state.ts_head = ts;
            self.inner.cond.notify_one();
            return Ok((Some(t), ts));
        }

        if state.ts_head == state.ts_tail {
            // we must be closed
            assert_eq!(state.closed, true);
            return Err(mpsc::RecvError);
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
            assert_eq!(state.closed, true);
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
    leave: mpsc::SyncSender<String>,
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
    pub fn new<S1: Into<String>, S2: Into<String>>(&self,
                                                   sender: S1,
                                                   receiver: S2)
                                                   -> (ClockedSender<T>, ClockedReceiver<T>) {
        let source = sender.into();
        let target = receiver.into();
        let send = ClockedSender {
            source: source.clone(),
            target: target.clone(),
            dispatcher: self.dispatcher.clone(),
        };
        let recv = ClockedReceiver::new(target.clone(), self.leave.clone(), self.bound);

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
    // the mutex is so that we can allow the control thread to have an &Target without T: Sync
    delayed: sync::Mutex<BinaryHeap<Delayed<T>>>,
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
    // needs to be locked so that control thread can access ReceiverInner Arcs
    targets: sync::Arc<sync::RwLock<HashMap<String, Target<T>>>>,
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
    id: String,
    // Sequence number counter
    counter: usize,
}

impl<T: Clone> DispatchInner<T> {
    /// Notifies all receivers of the given timestamp, and sends any given data to the intended
    /// recipients.
    ///
    /// If `data == None`, `ts` is sent all receivers.
    /// If `to == None`, `data.unwrap()` is sent to all receivers.
    /// If `to == Some(t)`, `data.unwrap()` is sent to the the receiver named `t`.
    fn notify(&self, to: Option<&String>, ts: usize, data: Option<T>) {
        let tgts = self.targets.read().unwrap();
        for (tn, t) in tgts.iter() {
            let mut state = t.channel.mx.lock().unwrap();
            debug!("{}: notifying {} about {}", self.id, tn, ts);
            if data.is_some() {
                if to.is_none() || to.unwrap() == tn.as_str() {
                    debug!("{}: including data", self.id);
                    while state.queue.len() == self.bound && !state.left {
                        state = t.channel.cond.wait(state).unwrap();
                    }

                    if state.left {
                        t.channel.cond.notify_one();
                        continue;
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
        self.freshness.values().min().map(|m| *m).unwrap_or(usize::max_value() - 1)
    }

    /// Should be called whenever the set of senders changes to process.
    /// Processes delayed messages that can now be delivered, and closes channels with no senders.
    fn senders_changed(&mut self, to: Option<String>, min_changed: bool) {
        debug!("{}: senders for {:?} changed", self.id, to);

        // if the removed sender has been delaying delivery of messages (by virtue of being the
        // sender with the lowest sequence number), we may now be able to send some more messages.
        // this can only happen if we're in forwarding mode, because in assignment mode, senders
        // don't have a way to not be up-to-date, and so there are no delayed messages.
        if self.forwarding.unwrap_or(false) && min_changed {
            self.process_delayed();
        }

        if self.broadcasters.is_empty() {
            // if there are broadcasters, no channel is closed
            let mut tgts = self.targets.write().unwrap();
            for (tn, t) in tgts.iter_mut()
                .filter(|&(_, ref t)| {
                    t.senders.is_empty() && t.delayed.lock().unwrap().is_empty()
                }) {
                debug!("{}: closing now-done channel {}", self.id, tn);
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
    fn process_delayed(&mut self) {
        let min = self.min();
        debug!("{}: processing delayed, min is {}", self.id, min);

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
            let next = self.bdelay.peek().map(|d| d.ts).unwrap_or(min + 1);
            debug!("{}: next from bcast is {:?}", self.id, next);
            // 2. finding the smallest in `targets[*].delayed`
            let tnext = {
                let tgts = self.targets.read().unwrap();
                let t = self.destinations
                    .iter()
                    .map(|to| {
                        let t = &tgts[to];
                        (to,
                         t.delayed
                            .lock()
                            .unwrap()
                            .peek()
                            .map(|d| d.ts)
                            .unwrap_or(min + 1))
                    })
                    .min_by_key(|&(_, ts)| ts);
                t.map(|(to, ts)| (to.to_owned(), ts))
            };
            debug!("{}: next from tdelay is {:?}", self.id, tnext);
            // 3. using the message from 2 if it is the earliest (and early enough)
            if let Some((to, tnext)) = tnext {
                if tnext <= next && tnext <= min {
                    debug!("{}: forwarding from tdelay", self.id);
                    let d = {
                        let tgts = self.targets.read().unwrap();
                        let mut x = tgts[to.as_str()].delayed.lock().unwrap();
                        x.pop().unwrap()
                    };
                    forwarded = d.ts;
                    self.notify(Some(&to), d.ts, Some(d.data));
                    continue;
                }
            }

            // 4. using the message from 1 (which must now be the earliest) if it is early enough
            if next <= min {
                debug!("{}: forwarding from bdelay", self.id);
                let d = self.bdelay.pop().unwrap();
                forwarded = d.ts;
                self.notify(None, d.ts, Some(d.data));
                continue;
            }

            // no delayed message has a sequence number <= min
            break;
        }

        debug!("{}: done replaying", self.id);

        // make sure all dependents know how up-to-date we are
        // even if we didn't send a delayed message for the min
        if forwarded < min && min != usize::max_value() - 1 {
            // if forwarded < min {
            debug!("{}: sending notify None for {} (since > {})",
                   self.id,
                   min,
                   forwarded);
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
                debug!("{}: got message with ts {:?} from {} for {:?}",
                       self.id,
                       td.ts,
                       td.from,
                       td.to);
                if self.forwarding.is_some() {
                    assert!(self.forwarding.unwrap() == td.ts.is_some(),
                            "one sender sent timestamp, another did not");
                } else {
                    self.forwarding = Some(td.ts.is_some())
                }

                let ts = td.ts.unwrap_or_else(|| {
                    self.counter += 1;
                    self.counter
                });
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
                            debug!("{}: delayed in {:?}", self.id, to);
                            let tgts = self.targets.read().unwrap();
                            tgts[to].delayed.lock().unwrap().push(Delayed {
                                ts: ts,
                                data: data,
                            });
                            drop(tgts);
                        } else {
                            debug!("{}: delayed in bcast", self.id);
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
                            let nmin = self.min();
                            // we *may* have increased the global min
                            if nmin > old {
                                self.process_delayed();
                            }
                        }
                    }
                }
            }
            Message::ReceiverJoin(name, inner) => {
                debug!("{}: receiver {} joined", self.id, name);
                if !self.destinations.insert(name.clone()) {
                    panic!("receiver {} already exists!", name);
                }

                let mut tgts = self.targets.write().unwrap();
                tgts.insert(name,
                            Target {
                                channel: inner,
                                senders: HashSet::new(),
                                delayed: sync::Mutex::new(BinaryHeap::new()),
                            });
            }
            Message::ReceiverLeave(name) => {
                debug!("{}: receiver {} left", self.id, name);
                // NOTE: Control thread has already unblocked senders and set .left

                // Deregister all senders (so they don't hold sending up due to freshness)
                let changed = {
                    let mut tgts = self.targets.write().unwrap();
                    let changed = {
                        let omin = self.min();
                        for s in tgts[&*name].senders.iter() {
                            self.freshness.remove(s.as_str());
                        }
                        self.min() != omin
                    };

                    // Deregister the receiver
                    tgts.remove(&*name);
                    self.destinations.remove(&*name);
                    changed
                };

                self.senders_changed(Some(name), changed);

                // TODO: ensure that subsequent send()'s return an error (somehow?) instead of just
                // crashing and burning (panic) like what happens now.
            }
            Message::SenderJoin(target, source) => {
                debug!("{}: sender {} for {:?} joined", self.id, source, target);

                if self.freshness.insert(source.clone(), 0).is_some() {
                    panic!("sender {} already exists!", source);
                }

                if let Some(target) = target {
                    let mut tgts = self.targets.write().unwrap();
                    tgts.get_mut(&*target).unwrap().senders.insert(source);
                } else {
                    self.broadcasters.insert(source);
                }
            }
            Message::SenderLeave(target, source) => {
                debug!("{}: sender {} for {:?} left", self.id, source, target);
                if let Some(ref target) = target {
                    // NOTE: target may not exist because receiver has left
                    let mut tgts = self.targets.write().unwrap();
                    if let Some(target) = tgts.get_mut(target.as_str()) {
                        target.senders.remove(&*source);
                    }
                    drop(tgts);
                } else {
                    self.broadcasters.remove(&*source);
                }

                let omin = self.min();
                self.freshness.remove(&*source);
                let nmin = self.min();
                self.senders_changed(target, omin != nmin);
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
    new_with_seed(bound, 0)
}

pub fn new_with_seed<T: Clone + Send + 'static>(bound: usize, seed: usize) -> Dispatcher<T> {
    use rand::{thread_rng, Rng};

    let (stx, srx) = mpsc::sync_channel(bound);
    let mut d = DispatchInner {
        targets: sync::Arc::new(sync::RwLock::new(HashMap::new())),
        destinations: HashSet::new(),
        bdelay: BinaryHeap::new(),
        broadcasters: HashSet::new(),
        freshness: HashMap::new(),
        forwarding: None,
        bound: bound,
        id: thread_rng().gen_ascii_chars().take(2).collect(),
        counter: seed,
    };

    let id = d.id.clone();
    let c_targets = d.targets.clone();
    let c_stx = stx.clone();
    let (ctx, crx) = mpsc::sync_channel::<String>(0);
    thread::spawn(move || {
        // this thread handles leaving receivers.
        // it is *basically* the following loop:
        //
        // ```
        // for left in crx {
        //   c_targets[left].channel.left = true;
        //   ctx.send(ReceiverLeave(left));
        // }
        // ```
        //
        // unfortunately, it gets complicated by two factors:
        //
        //  - if a receiver is created and then dropped immediately, the Leave could reach us
        //    before the Join reaches the dispatcher. in this case, targets[left] doesn't exist
        //    yet. we thus need to wait for the dispatcher to catch up to the join.
        //
        //  - the dispatcher can't be blocking on a send to the receiver we are dropping because
        //    it doesn't know about it yet, and thus must process the join message before it can
        //    block waiting on us). however, there *is* a possibility of the dispatcher being
        //    blocked on a send to a channel that is queued to be dropped *behind* this one. this
        //    is the source of much of the complexity below.
        //
        // essentially, we keep track of leaving receivers that we haven't successfully handled
        // yet, but also keep reading from crx to see if there are other receivers that are also
        // trying to leave.

        // receivers that are trying to leave
        let mut leaving = Vec::new();
        // temp for keeping track of nodes are *still* trying to leave while draining `leaving`
        let mut leaving_ = Vec::new();

        'recv: loop {
            // are there more receivers trying to leave?
            let left = crx.try_recv();
            match left {
                Ok(left) => {
                    // yes -- deal with them too
                    leaving.push(left);
                }
                Err(..) if !leaving.is_empty() => {
                    // no, but deal with the receivers that wanted to leave
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    // no, and there will never be more
                    // we must also have dealt with all receivers who tried to leave
                    // it's safe for us to exit
                    break 'recv;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    // no, and there also aren't any for us to retry
                    // to avoid busy looping, we can now do a blocking receive
                    let left = crx.recv();
                    if let Ok(left) = left {
                        // someone tried to leave -- let's try to deal with that
                        leaving.push(left);
                    } else {
                        // channel closed, and no one is waiting -- safe to exit
                        break 'recv;
                    }
                }
            }

            // try to process any receivers trying to leave
            for left in leaving.drain(..) {
                debug!("{} control: dealing with departure of receiver {}",
                       id,
                       left);

                let targets = c_targets.read().unwrap();
                if let Some(t) = targets.get(&*left) {
                    // the receiver exists, so we can remove it
                    let mut state = t.channel.mx.lock().unwrap();
                    state.left = true;
                    state.closed = true;
                    t.channel.cond.notify_one();
                    drop(state);

                    // kick off a message to the dispatcher in the background.
                    // we don't want it to be in the foreground, because we may have other things
                    // to close that could block sending to the dispatcher.
                    let ctx = c_stx.clone();
                    thread::spawn(move || {
                        ctx.send(Message::ReceiverLeave(left)).unwrap();
                    });
                } else {
                    // dispatcher doesn't know about this receiver yet
                    leaving_.push(left);
                }
            }
            leaving.extend(leaving_.drain(..));
        }
    });

    thread::spawn(move || {
        for m in srx.iter() {
            d.absorb(m);
        }
    });

    Dispatcher {
        dispatcher: stx,
        leave: ctx,
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
        .map(|i| dtx.clone(format!("{}_to_fuse_{}", sources[i].name, i)))
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
    fn recv_drop_unblocks_sender() {
        use std::thread;
        use std::time::Duration;

        // Create a dispatcher
        let d = super::new(1);

        // Create two channels
        let (tx_a, rx_a) = d.new("atx", "arx");
        let (tx_b, rx_b) = d.new("btx", "brx");

        // Make tx_a a broadcaster (so it would block on b)
        // Note that we have to do this *before* we saturate the channel to the dispatcher
        let tx_a = tx_a.to_broadcaster();

        // Fill rx_b
        thread::spawn(move || {
            for _ in 0..20 {
                tx_b.send("b");
            }
        });
        thread::sleep(Duration::from_millis(200));

        // Drop b's receiver
        drop(rx_b);

        // All of tx_b's sends should be dropped, and tx_a should be able to send
        tx_a.broadcast("a");

        // And that messages are still delivered
        loop {
            let rx = rx_a.recv();
            assert!(rx.is_ok());
            let rx = rx.unwrap();
            if rx.0.is_some() {
                assert_eq!(rx.0, Some("a"));
                break;
            }
        }
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
        // note that dropping the receiver kills the senders too!

        // And that messages are still delivered
        assert_eq!(rx_b.recv(), Ok((Some(10), 1)));
    }

    #[test]
    fn forward_with_no_senders() {
        use std::sync::mpsc;

        let d = super::new(1);
        let (tx_a, rx_a) = d.new("atx", "arx");
        let (tx_b, rx_b) = d.new("btx", "brx");

        tx_a.forward(Some(1), 1);
        // the message is queued because tx_b hasn't sent anything

        // drop both senders, freeing Some(1) from the delayed queue. we specifically want to
        // explore the case where Some(1) is freed when there are no senders, so we have to drop
        // tx_a first (since tx_b is holding up the system).
        drop(tx_a);
        drop(tx_b);

        // Ensure that receiver still gets notified of messages
        assert_eq!(rx_a.recv(), Ok((Some(1), 1)));

        // And that other still gets a None
        assert_eq!(rx_b.recv(), Ok((None, 1)));

        // And that no more entries are sent
        assert_eq!(rx_a.recv(), Err(mpsc::RecvError));
        assert_eq!(rx_b.recv(), Err(mpsc::RecvError));
    }

    #[test]
    fn broadcast_dupe_termination() {
        use std::sync::mpsc;

        let d = super::new(1);
        let (tx, rx) = d.new("tx", "rx");
        let tx = tx.to_broadcaster();

        tx.broadcast_forward(Some("a"), 10);
        tx.broadcast_forward(Some("b"), 10);
        drop(tx);

        assert_eq!(rx.recv(), Ok((Some("a"), 10)));
        assert_eq!(rx.recv(), Ok((Some("b"), 10)));
        assert_eq!(rx.recv(), Err(mpsc::RecvError));
    }

    fn fused_setup<T, F, G>(a: F,
                            b: G)
                            -> (super::Dispatcher<T>,
                                (super::ClockedSender<T>, super::Dispatcher<T>),
                                (super::ClockedSender<T>, super::Dispatcher<T>),
                                super::ClockedReceiver<T>)
        where F: Send + 'static + Fn(Option<T>, usize, &super::ClockedBroadcaster<T>),
              G: Send + 'static + Fn(Option<T>, usize, &super::ClockedBroadcaster<T>),
              T: Send + Clone
    {
        use std::thread;

        // Here we construct a graph like this:
        //
        //     a_in --+                +-- b_in
        //       :    |                |     :
        //       :    +-> dispatcher <-+     :
        //       :         |      |          :
        //       a <-------+      +--------> b
        //       |                           |
        //       v                           v
        //   dispatcher                  dispatcher
        //       |                           |
        //       + broadcast       broadcast +
        //       |                           |
        //       +---------> fuse <----------+
        //                    |
        //                    v
        //                    c
        //
        // a and b both broadcast to allow other c-like nodes to also subscribe to their updates
        // (this is what the dispatchers hanging of a and b are for), though this is not used in
        // the graph above. The dispatcher at the top assigns sequence numbers (i.e., a_in and b_in
        // use send()).

        let d = super::new(20);
        let (a_in, rx_a) = d.new("a_in", "rx_a");
        let (b_in, rx_b) = d.new("b_in", "rx_b");

        let ad = super::new(20);
        let (tx_a, a_out) = ad.new("tx_a", "a_out");
        thread::spawn(move || {
            let tx = tx_a.to_broadcaster();
            for (x, ts) in rx_a {
                a(x, ts, &tx);
            }
        });

        let bd = super::new(20);
        let (tx_b, b_out) = bd.new("tx_b", "b_out");
        thread::spawn(move || {
            let tx = tx_b.to_broadcaster();
            for (x, ts) in rx_b {
                b(x, ts, &tx);
            }
        });

        let c_in = super::fuse(vec![a_out, b_out], 20);
        (d, (a_in, ad), (b_in, bd), c_in)
    }

    fn fw<T: Send + Clone>(x: Option<T>, ts: usize, tx: &super::ClockedBroadcaster<T>) {
        tx.broadcast_forward(x, ts);
    }

    #[test]
    fn deep_fuse_simple() {
        use std::sync::mpsc;

        let (_, (a_in, _), (b_in, _), c_in) = fused_setup(fw, fw);

        a_in.send("a1");

        // the send should also cause b to receive a None
        // a and b should thus broadcast with the same ts
        // which brings all inputs of the fuse up to date
        // so the fuse should output a Some("a").
        assert_eq!(c_in.recv().unwrap().0, Some("a1"));

        // since the fuse output something for the latest ts
        // it should not also send a None with the same ts
        assert_eq!(c_in.try_recv(), Err(mpsc::TryRecvError::Empty));

        a_in.send("a2");
        b_in.send("b");

        let c1 = c_in.recv().unwrap();
        let c2 = c_in.recv().unwrap();
        assert!(c1.0.is_some());
        assert!(c2.0.is_some());
        assert!(c2.1 > c1.1);
    }

    #[test]
    fn deep_fuse_busy() {
        use std::thread;

        let (_, (a_in, _), (b_in, _), c_in) = fused_setup(fw, fw);

        thread::spawn(move || {
            for i in 0..100 {
                if i % 2 == 0 {
                    a_in.send("ax");
                } else {
                    b_in.send("bx");
                }
            }
        });

        let mut n = 0;
        let mut old = 0;
        for (c, ts) in c_in {
            assert!(ts > old);
            assert!(c.is_some());
            old = ts;
            n += 1;
        }
        assert_eq!(n, 100);
    }

    #[test]
    fn deep_fuse_busy_lossy() {
        use std::thread;

        let a = |x, ts, tx: &super::ClockedBroadcaster<_>| {
            if ts % 2 == 0 {
                tx.broadcast_forward(None, ts);
            } else {
                tx.broadcast_forward(x, ts);
            }
        };
        let b = |x, ts, tx: &super::ClockedBroadcaster<_>| {
            // note that % is different so that there will be some timestamps where one drops, but
            // the other does not.
            if ts % 3 == 0 {
                tx.broadcast_forward(None, ts);
            } else {
                tx.broadcast_forward(x, ts);
            }
        };

        let (_, (a_in, _), (b_in, _), c_in) = fused_setup(a, b);

        thread::spawn(move || {
            for i in 0..100 {
                if i % 2 == 0 {
                    a_in.send("ax");
                } else {
                    b_in.send("bx");
                }
            }
        });

        let mut old = 0;
        for (c, ts) in c_in {
            assert!(ts > old);
            if ts % 2 == 0 && ts % 3 == 0 {
                // both dropped, so must be None
                assert!(c.is_none());
            } else if ts % 2 == 0 {
                // a would have dropped an ax
                assert!(c.is_none() || c.unwrap() == "bx");
            } else if ts % 3 == 0 {
                // b would have dropped a bx
                assert!(c.is_none() || c.unwrap() == "ax");
            }
            old = ts;
        }
    }

    #[test]
    fn deep_fuse_dupe() {
        use std::thread;

        let a = |x: Option<_>, ts, tx: &super::ClockedBroadcaster<_>| {
            if x.is_some() && ts % 2 == 0 {
                tx.broadcast_forward(Some("axx"), ts);
                // so two sends for the same ts
            }
            tx.broadcast_forward(x, ts);
        };
        let b = |x: Option<_>, ts, tx: &super::ClockedBroadcaster<_>| {
            if x.is_some() && ts % 2 == 0 {
                tx.broadcast_forward(Some("bxx"), ts);
                // so two sends for the same ts
            }
            tx.broadcast_forward(x, ts);
        };

        let (_, (a_in, _), (b_in, _), c_in) = fused_setup(a, b);

        thread::spawn(move || {
            for i in 0..100 {
                if i % 2 == 0 {
                    a_in.send("ax");
                } else {
                    b_in.send("bx");
                }
            }
        });

        let mut n = 2;
        let mut old = 0;
        for (c, ts) in c_in {
            assert!(ts >= old); // note >= for dupes
            assert!(c.is_some());
            if ts != old && n != 0 {
                assert_eq!(n, 2);
                n = 0;
            }

            if ts % 2 == 0 {
                n += 1;
            }
            old = ts;
        }

        if n != 0 {
            assert_eq!(n, 2);
        }
    }

    #[test]
    fn multisend_thread_interleaving() {
        use std::thread;

        for _ in 0..1000 {
            let d = super::new(20);
            let (tx_a, rx) = d.new("tx_a", "rx");
            let tx_b = tx_a.clone("tx_b");

            let t_a = thread::spawn(move || {
                tx_a.forward(Some("c_1"), 1);
                tx_a.forward(None, 2); // absorbed
                tx_a.forward(Some("c_3"), 3);
                tx_a.forward(Some("a_1"), 5);
                tx_a.forward(Some("a_2"), 6);
            });
            let t_b = thread::spawn(move || {
                tx_b.forward(Some("c_1"), 1);
                tx_b.forward(Some("c_2"), 2);
                tx_b.forward(None, 3); // absorbed
                tx_b.forward(Some("b_1"), 4);
                tx_b.forward(None, 6);
            });

            assert_eq!(rx.recv(), Ok((Some("c_1"), 1)));
            assert_eq!(rx.recv(), Ok((Some("c_1"), 1))); // received from both ins, so duped
            assert_eq!(rx.recv(), Ok((Some("c_2"), 2)));
            assert_eq!(rx.recv(), Ok((Some("c_3"), 3)));
            assert_eq!(rx.recv(), Ok((Some("b_1"), 4)));
            assert_eq!(rx.recv(), Ok((Some("a_1"), 5)));

            t_a.join().unwrap();
            t_b.join().unwrap();
        }
    }

    #[test]
    fn fuse_broadcast() {
        use std::thread;

        for _ in 0..1 {
            let a_cd = super::new(20);
            let b_cd = super::new(20);

            let (a_tx, x) = a_cd.new("a_tx", "_");
            let a_tx = a_tx.to_broadcaster();
            drop(x);

            let (b_tx, x) = b_cd.new("b_tx", "_");
            let b_tx = b_tx.to_broadcaster();
            drop(x);

            let (x, c_in_a) = a_cd.new("_", "c");
            let (y, c_in_b) = b_cd.new("_", "c");
            let c = super::fuse(vec![c_in_a, c_in_b], 20);
            drop(x);
            drop(y);

            let t_a = thread::spawn(move || {
                a_tx.broadcast_forward(Some("c_1"), 1);
                a_tx.broadcast_forward(Some("c_2"), 2);
                a_tx.broadcast_forward(None, 3);
                a_tx.broadcast_forward(Some("a_1"), 5);
                a_tx.broadcast_forward(Some("a_2"), 6);
            });
            let t_b = thread::spawn(move || {
                b_tx.broadcast_forward(None, 1);
                b_tx.broadcast_forward(Some("c_2"), 2);
                b_tx.broadcast_forward(Some("c_3"), 3);
                b_tx.broadcast_forward(Some("b_1"), 4);
                b_tx.broadcast_forward(None, 6);
            });

            assert_eq!(c.recv(), Ok((Some("c_1"), 1)));
            assert_eq!(c.recv(), Ok((Some("c_2"), 2)));
            assert_eq!(c.recv(), Ok((Some("c_2"), 2))); // received from both ins, so duped
            assert_eq!(c.recv(), Ok((Some("c_3"), 3)));
            assert_eq!(c.recv(), Ok((Some("b_1"), 4)));
            assert_eq!(c.recv(), Ok((Some("a_1"), 5)));

            t_a.join().unwrap();
            t_b.join().unwrap();
        }
    }

    #[test]
    fn joining_fused_quiet_sender() {
        use std::thread;
        use std::sync::mpsc;
        use std::time::Duration;

        let (_, (a_in, _), (b_in, _), c_in) = fused_setup(fw, fw);

        // start a long-running sender on a
        thread::spawn(move || {
            for i in 0..10000 {
                a_in.send(i);
            }
        });

        // quiet node b should not prevent c from finishing
        let mut prev = None;
        let mut waits = 0;
        loop {
            let r = c_in.try_recv();
            if let Err(mpsc::TryRecvError::Disconnected) = r {
                break;
            }
            if let Ok((Some(i), _)) = r {
                if let Some(ref pi) = prev {
                    assert_eq!(*pi + 1, i);
                }
                prev = Some(i);
                waits = 0;

                if i == 9999 {
                    // sender has finished
                    // should now be safe to consume the channel
                    // once b has been dropped
                    drop(b_in);
                    c_in.count();
                    break;
                }

                continue;
            }

            // nothing in the channel; let's wait for a little while
            // unless we've already waited for way too long
            assert!(waits < 100);
            thread::sleep(Duration::from_millis(1));
            waits += 1;
        }
        assert!(prev.is_some());
    }

    #[test]
    fn joining_adjacent() {
        use std::thread;
        use std::sync::mpsc;
        use std::time::Duration;

        let (d, (a_in, _), (b_in, bd), ab_in) = fused_setup(fw, fw);

        // start a receiver for ab to avoid blocking any inputs
        let ab_t = thread::spawn(move || {
            ab_in.count();
        });

        // send some stuff on a and b
        a_in.send("a1");
        b_in.send("b1");

        // sleep a while to avoid race where bc sees b1
        thread::sleep(Duration::from_millis(500));

        // add another base input
        let (c_in, rx_c) = d.new("c_in", "rx_c");
        let cd = super::new(20);
        let (tx_c, c_out) = cd.new("tx_c", "c_out");
        thread::spawn(move || {
            let tx = tx_c.to_broadcaster();
            for (x, ts) in rx_c {
                tx.broadcast_forward(x, ts);
            }
        });

        // add another node that fuses b and c
        // 1. get another out from b
        let (b_tmp, b_out) = bd.new("bc_tx", "bc_rx");
        drop(b_tmp);
        let bc_in = super::fuse(vec![b_out, c_out], 20);

        // check that sends from b and c both get to bc
        b_in.send("b2");
        c_in.send("c1");
        assert!(bc_in.recv().unwrap().0.is_some());
        assert!(bc_in.recv().unwrap().0.is_some());
        assert_eq!(bc_in.try_recv(), Err(mpsc::TryRecvError::Empty));

        // dropping all ab inputs should free up ab receiver
        drop(a_in);
        drop(b_in);
        ab_t.join().unwrap();

        // but should not free up bc receiver
        assert_eq!(bc_in.try_recv(), Err(mpsc::TryRecvError::Empty));

        // dropping c should finally free up bc
        drop(c_in);
        assert_eq!(bc_in.recv(), Err(mpsc::RecvError));
    }
}
