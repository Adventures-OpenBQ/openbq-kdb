"""Bounded diagnostics transport. Output workers never touch PyKX."""
from __future__ import annotations

import json
import logging
import os
import queue
import select
import threading


class RuntimeOutputDrain:
    """Continuously discard raw child bytes; retain only safe classifications.

    Unstructured runtime/license output has no reliable secret grammar. Therefore
    no raw child text is retained or forwarded. The bounded startup tail contains
    fixed diagnostic codes and byte counts, never excerpts.
    """
    def __init__(self, stream, limit=32768):
        self.stream = stream
        self.limit = min(32768, max(1, limit))
        self.bytes_read = 0
        self.tail_bytes = bytearray()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name='kdb-runtime-drain', daemon=True)

    def start(self):
        self._thread.start()
        return self

    def _run(self):
        try:
            descriptor = self.stream.fileno()
            os.set_blocking(descriptor, False)
            while not self._stop.is_set():
                ready, _, _ = select.select([descriptor], [], [], .05)
                if not ready:
                    continue
                try:
                    data = os.read(descriptor, 8192)
                except BlockingIOError:
                    continue
                if not data:
                    break
                self.bytes_read += len(data)
                code = b'KDB_RUNTIME_OUTPUT_REDACTED\n'
                self.tail_bytes.extend(code)
                del self.tail_bytes[:-self.limit]
        except (OSError, ValueError):
            pass
        finally:
            self.stream.close()

    def summary(self):
        return ('KDB_RUNTIME_OUTPUT_REDACTED bytes=' + str(self.bytes_read)) if self.bytes_read else ''

    def close(self, timeout=.2):
        self._stop.set()
        self._thread.join(timeout)


class DiagnosticPump:
    """Poll plain records on the session owner, emit off-thread through logging.

    Enable explicitly with an identity granted diagnostics. Queue overflow drops
    operational events with an observable counter; it never blocks ingestion.
    A blocked logger cannot be interrupted by Python, so its worker is a daemon
    and close is bounded. At most one output worker is started per pump instance.
    """
    def __init__(self, store, *, logger=None, interval=1., queue_size=1000):
        self.store = store
        self.logger = logger if logger is not None else logging.getLogger('openbq_kdb.diagnostics')
        self.interval = max(.001, interval)
        self._queue = queue.Queue(maxsize=min(1000, max(1, queue_size)))
        self._stop = threading.Event()
        self._poller = None
        self._worker = None
        self.cursor = 0
        self.boot_id = None
        self.dropped = 0
        self.overwritten = 0
        self.failures = 0

    @property
    def pending(self):
        return self._queue.qsize()

    def start(self):
        if self._poller is not None:
            return self
        self._worker = threading.Thread(target=self._output, name='kdb-diagnostic-output', daemon=True)
        self._poller = threading.Thread(target=self._poll, name='kdb-diagnostic-poll', daemon=True)
        session = getattr(self.store, 'session', None)
        if session is not None:
            session._diagnostic_sink = self.enqueue
        self._worker.start()
        self._poller.start()
        return self

    def _poll(self):
        while not self._stop.is_set():
            try:
                result = self.store.read_diagnostics(self.cursor, 100)
                boot = result['boot_id']
                if self.boot_id is not None and self.boot_id != boot:
                    self.cursor = 0
                    self.boot_id = boot
                    continue  # reread the new boot from zero, not the old cursor
                self.boot_id = boot
                self.overwritten = int(result['overwritten'])
                for raw in result['events'][:100]:
                    self.enqueue(raw)
                self.cursor = int(result['next_sequence'])
            except Exception:
                # No unfiltered error text, credentials or raw payloads in output.
                self.failures += 1
            self._stop.wait(self.interval)

    def enqueue(self, raw):
        """Nonblocking plain-record admission, also used by session lifecycle events."""
        try:
            record = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
            if not isinstance(record, dict):
                return
            encoded = json.dumps(record, separators=(',', ':'), allow_nan=False)
            if len(encoded.encode()) > 4096:
                self.dropped += 1
                return
            self._queue.put_nowait((int(record.get('level', 20)), encoded))
        except queue.Full:
            self.dropped += 1
        except (TypeError, ValueError):
            self.failures += 1

    def _output(self):
        while not self._stop.is_set():
            try:
                level, encoded = self._queue.get(timeout=.05)
            except queue.Empty:
                continue
            try:
                self.logger.log(level, '%s', encoded)
            except Exception:
                self.failures += 1
            finally:
                self._queue.task_done()

    def close(self, timeout=.5):
        session = getattr(self.store, 'session', None)
        if session is not None and session._diagnostic_sink == self.enqueue:
            session._diagnostic_sink = None
        self._stop.set()
        for thread in (self._poller, self._worker):
            if thread is not None:
                thread.join(timeout / 2)
