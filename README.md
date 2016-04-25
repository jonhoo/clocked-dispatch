[![Build Status](https://travis-ci.org/jonhoo/clocked-dispatch.svg?branch=master)](https://travis-ci.org/jonhoo/clocked-dispatch)

# Clocked dispatch service in Rust

This Rust crate provides a clocked message dispatch service. Clocked
dispatch is, at its heart, simply a set of channels. The difference is
that each channel is aware of how many messages the system as a whole
has processed. This can be useful to have different threads know how
up-to-date another thread is.

For example, say a message `m` is sent to some receiver `r`. Another
receiver `r'` who does not receive `m` will still be made aware that one
message has been dispatched when it does a subsequent read.

To help illustrate why this is useful, consider two channel pairs, one
between `s1` and `r1`, and one between `s2` and `r2`. Assume that `r1`
and `r2` occasionally need to communicate. However, they want to wait
until the other has seen any updates preceding those that they have
received. Say that `r1` has seen 7 updates, but `r2` has not seen any.
Is `r2` sufficiently up-to-date? Well, it could be -- all the channel
sends in the system could have been for `r1`!

Clocked dispatch simplifies by introducing a shared dispatcher that
assigns monotonically increasing sequence numbers to all messages. It
looks something like this:

```
  s1       s2
   +---+----+
       |
       + dispatcher
       |
   +---+----+
  r1       r2
```

When `s1` wishes send a message `m` to `r1`, it instead sends `m` to the
dispatcher. The dispatcher assigns a sequence number to `m`, forwards it
to `r1`, **and then sends a clock update to `r2`**. This clock update
allows `r2`, in the scenario described above, to see that 7 updates have
passed it by, telling it that it has seen all the updated that `r1` has
seen.

The dispatcher introduces another subtle problem. Specifically, if `r2`
is slow to accept updates, it could block the dispatcher when it tries
to send clock updates. To avoid this, the library implements a custom
channel that conveys these sequence numbers without blocking the sender.
Blocking will still happen if a message is being sent, but timestamp
updates will not block the dispatcher. This means that `s1.send(v)` will
not block even though `r2` is not currently reading from its input
channel.

## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms
or conditions.
