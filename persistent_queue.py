# Copyright 2019 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Durable, persistent log / FIFO queue."""

import pickle
import threading

import apsw

_CREATE_TABLE_LOG = """
CREATE TABLE IF NOT EXISTS log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  data BLOB
)
"""
_CREATE_TABLE_CLIENTS = """
CREATE TABLE IF NOT EXISTS clients (
  client_id TEXT PRIMARY KEY,
  acknowledged INTEGER
)
"""
_INSERT = """INSERT INTO log (data) VALUES (:data)"""
_QUERY = """
SELECT l.id, l.data
FROM log AS l
WHERE
  l.id > COALESCE((SELECT c.acknowledged FROM clients AS c WHERE c.client_id = :clientid), -1)
ORDER BY l.id ASC
"""
_INITIALIZE_CLIENT = """
INSERT OR IGNORE INTO clients (client_id, acknowledged) VALUES (:clientid, -1)
"""
_ADVANCE_CLIENT = """
UPDATE clients SET acknowledged = MAX(acknowledged, :acknowledged) WHERE client_id = :clientid
"""


class Queue(object):
  """Persistent, disk-based queue.

  It has to be used as a context. On entering the context the underlying
  database file is opened and on exiting it it's closed.
  """

  def __init__(self, database_filename, wal_autocheckpoint=None):
    """Initialize the queue to use `database_filename` for storage."""
    self._database_filename = database_filename
    self._wal_autocheckpoint = wal_autocheckpoint
    self._connection = None
    self._available = threading.Condition()

  def __enter__(self):
    flags = apsw.SQLITE_OPEN_READWRITE | apsw.SQLITE_OPEN_CREATE
    self._connection = apsw.Connection(self._database_filename, flags=flags)
    cursor = self._connection.cursor()
    try:
      if self._wal_autocheckpoint is not None:
        cursor.execute("PRAGMA journal_mode=wal")
        self._connection.wal_autocheckpoint(self._wal_autocheckpoint)
      cursor.execute(_CREATE_TABLE_LOG)
      cursor.execute(_CREATE_TABLE_CLIENTS)
    finally:
      cursor.close()
    return self

  def __exit__(self, exc_type, exc, tb):
    self._connection.close()

  def _execute_query(self, statements, bindings=None):
    cursor = self._connection.cursor()
    try:
      for row in cursor.execute(statements, bindings):
        yield row
    finally:
      cursor.close()

  def _execute(self, statements, bindings=None):
    for _ in self._execute_query(statements, bindings=bindings):
      pass

  def trigger(self):
    """Notifies clients that there might be messages available in the queue.

    This forces them to wake up and query the persistent storage. In most cases
    this is not needed, as the `put` method already calls this. It only makes
    sense to call this method if there are multiple processes accessing the
    queue and it is known that another process has entered data in the queue
    (for example by some means of IPC).
    """
    with self._available:
      self._available.notify_all()

  def put(self, message):
    """Store `message` in the queue.

    After the method returns, it is guaranteed that the message has been
    persisted to the queue and will be available to `get` even in the case of a
    crash.

    Args:
      message: An message to be stored in the queue.
    """
    data = pickle.dumps(message)
    self._execute(_INSERT, {"data": data})
    self.trigger()

  def get(self, client=None):
    """Fetch messages in the queue not yet processed by `client`.

    An message is acknowledged by the next call to 'next' on the returned
    generator. In practice this means that when processing the generator in a
    `for` loop, the body of the loop needs to finish without a `break` or an
    exception in order to acknowledge the current message. This ensures that
    messages don't get lost if their processing is interrupted.

    This can be also used to peek for the next element in the queue without
    consuming it, for example:

      for message in queue.get():
        # An message is available, do something with it.
        break  # By terminating the loop the message won't be acknowledged.

    Args:
      client: A string that identifies this client. Two clients with a
        different ID consume the queue independently. If `None`, an empty string
        is used.

    Yields: messages currently available in the queue for `client`.
    """
    if client is None:
      client = ""
    client = str(client)
    self._execute(_INITIALIZE_CLIENT, {"clientid": client})
    for (rowid, data) in self._execute_query(_QUERY, {"clientid": client}):
      yield pickle.loads(data)
      self._execute(_ADVANCE_CLIENT, {
          "clientid": client,
          "acknowledged": rowid
      })

  def get_blocking(self, client=None, tick=None):
    """Fetch messages in the queue as they become available to `client`.

    Very similar to `get`, but the generator never returns and waits for new
    messages to become available, if there currently none available to `client`
    in the queue.

    Args:
      client: A string that identifies this client. Two clients with a
        different ID consume the queue independently. If `None`, an empty string
        is used.
      tick: How often (in seconds) to query the database for new data. If
        messages are inserted in the queue from the same process, blocked
        threads are woken up automatically. But if they are inserted from a
        different process, they will be noticed only during the next database
        query.  Making `tick` smaller ensures that such messages are processed
        earlier, at the cost of more frequent database communication.

    Yields: messages from the queue as they become available to `client`.
    """
    while True:
      for message in self.get(client):
        yield message
      with self._available:
        self._available.wait(timeout=tick)
