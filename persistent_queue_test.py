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

from concurrent import futures
import os
import signal
import tempfile
import time
import unittest

import persistent_queue


class TestQueueSingleProcess(unittest.TestCase):

  def test_empty(self):
    with persistent_queue.Queue(":memory:") as q:
      self.assertEqual(list(q.get()), [])

  def test_put_single(self):
    with persistent_queue.Queue(":memory:") as q:
      q.put(42)
      self.assertEqual(list(q.get()), [42])

  def test_put_none(self):
    with persistent_queue.Queue(":memory:") as q:
      q.put(None)
      self.assertEqual(list(q.get()), [None])

  def test_put_with_get(self):
    with persistent_queue.Queue(":memory:") as q:
      q.put(42)
      q.put(43)
      self.assertEqual(list(q.get()), [42, 43])
      q.put(12)
      q.put(13)
      self.assertEqual(list(q.get()), [12, 13])

  def test_interrupted_get(self):
    with persistent_queue.Queue(":memory:") as q:
      for x in [42, 43, 44]:
        q.put(x)
      for x in q.get():
        if x == 42:
          continue
        if x == 43:
          break
        self.fail("Unexpected value: {}".format(x))
      self.assertEqual(list(q.get()), [43, 44])

  def test_get_blocking_wakes_up(self):
    with persistent_queue.Queue(":memory:") as q:
      with futures.ThreadPoolExecutor(max_workers=2) as executor:

        def consumer():
          self.assertEqual(next(q.get_blocking(tick=5)), "foo")

        c = executor.submit(consumer)
        # Give it enough time to create the database and wait.
        time.sleep(0.4)

        def producer():
          q.put("foo")

        p = executor.submit(producer)
        p.result()
        # The consumer now should finish immediately.
        # Let's give it a short grace period.
        start = time.time()
        c.result()
        self.assertLess(time.time() - start,
                        1,
                        msg="Consumer wasn't triggered soon enough")

  def test_concurrent_load(self):
    count = 20
    with persistent_queue.Queue(":memory:") as q:

      def consumer(client):
        collected = []
        while len(collected) < count * count:
          # TODO
          collected.extend(q.get(client=client))
          #time.sleep(1)
        collected.sort(key=lambda pair: pair[0])
        self.assertEqual(collected,
                         [(id, i) for id in range(count) for i in range(count)])

      def producer(tag):
        for i in range(count):
          q.put((tag, i))

      with futures.ThreadPoolExecutor(max_workers=2 * count) as executor:
        consumers = []
        for client in range(count):
          c = executor.submit(consumer, client)
          consumers.append(c)
        producers = []
        for tag in range(count):
          p = executor.submit(producer, tag)
          producers.append(p)
        # Wait for all to finish successfully.
        for p in producers:
          p.result()
        for c in consumers:
          c.result()


class TestQueueMultiProcess(unittest.TestCase):

  @staticmethod
  def _kill_self():
    """Kill the current process as hard as possible."""
    os.kill(os.getpid(), signal.SIGKILL)

  @staticmethod
  def _put_and_die(database, elements):
    """Put given elements into the database and die horribly."""
    with persistent_queue.Queue(database) as q:
      for item in elements:
        q.put(item)
      TestQueueMultiProcess._kill_self()

  @staticmethod
  def _get_and_die(database, last, tick=0.5):
    """Get elements until `last` is seen and die horribly."""
    with persistent_queue.Queue(database) as q:
      for item in q.get_blocking(tick=tick):
        if item == last:
          TestQueueMultiProcess._kill_self()

  def test_killed_during_get(self):
    with tempfile.TemporaryDirectory() as directory:
      database = os.path.join(directory, "database")
      with persistent_queue.Queue(database) as q:
        for x in [42, 43, 44]:
          q.put(x)

      with futures.ProcessPoolExecutor(max_workers=1) as executor:
        executor.submit(TestQueueMultiProcess._get_and_die, database,
                        43).exception()

      with persistent_queue.Queue(database) as q:
        self.assertEqual(list(q.get()), [43, 44])

  def test_killed_after_put(self):
    with tempfile.TemporaryDirectory() as directory:
      database = os.path.join(directory, "database")

      with futures.ProcessPoolExecutor(max_workers=1) as executor:
        executor.submit(TestQueueMultiProcess._put_and_die, database,
                        [42]).exception()

      with persistent_queue.Queue(database) as q:
        self.assertEqual(list(q.get()), [42])


if __name__ == '__main__':
  unittest.main()
