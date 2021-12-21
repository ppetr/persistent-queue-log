# Durable, persistent log / FIFO queue.

*Disclaimer: This is not an official Google product.*

[![Build Status](https://travis-ci.com/ppetr/persistent-queue-log.svg?branch=master)](https://travis-ci.com/ppetr/persistent-queue-log)

Persistent log/queue implemented in Python. Messages are kept indefinitely by
default (as in a log). For each client it's last seen message is recorded,
therefore each client receives each message just once (queue API).

Goals:

  - "Put" durability: Once an message is stored in the queue, it remains there
    even in the case of a crash or a power failure.
  - "Get" durability: An message is considered to be consumed only when the
    caller that acknowledged it has fully processed it. If a crash occurs before
    such an acknowledgement, the message is considered unprocessed and is passed
    to the client again.
  - Allows multiple clients (distinguished by a name) to process messages
    independently. Acknowledging a message by one client still keeps it
    available for another one.
  - Keep data possibly indefinitely.
  - Thread-safe interface.
  - Simplicity, as little dependencies as possible.

Non-goals:

  - Sacrifice durability for performance. There is usually a trade-off between
    durability and performance. This library always stays on the side of
    durability.
  - Unique messages. While the library tries to make sure each message is
    delivered only once, this cannot be guaranteed (unless durability is
    sacrificed). Therefore callers of `get` need to be prepared for the
    possibility of receving single a message multiple times in rare cases.

## Dependencies

- Python 3.6+
- SQLite3
- [APSW](https://rogerbinns.github.io/apsw/) SQLite 3 wrapper

On Debian based systems this can be installed (for Python 3) by

    sudo apt-get install python3-apsw

## Implementation details

The queue is implemented on top of a dedicated SQL database. APSW is used mainly
because it already a provides thread-safe interface to SQLite.

Messages stored in the queue are (de)serialized using the 'pickle' library.
Currently there is no way of providing custom serializer.
