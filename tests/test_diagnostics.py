"""Bounded diagnostics and byte-only child-output collection."""
import json
import subprocess
import sys
import threading
import time

from openbq_kdb.diagnostics import DiagnosticPump, RuntimeOutputDrain


def test_runtime_drain_bounds_and_redacts_noisy_child():
    proc = subprocess.Popen([sys.executable, '-c', 'import sys;sys.stdout.buffer.write(b"SECRET_LICENSE_TOKEN"*200000)'], stdout=subprocess.PIPE)
    drain = RuntimeOutputDrain(proc.stdout)
    drain.start()
    assert proc.wait(timeout=5) == 0
    drain.close()
    assert drain.bytes_read > 1_000_000
    assert len(drain.tail_bytes) <= 32768
    assert 'SECRET' not in drain.summary()
    assert drain.summary().startswith('KDB_RUNTIME_OUTPUT_REDACTED')


def test_pump_queue_and_hung_output_never_block_polling():
    blocked = threading.Event()
    entered = threading.Event()
    class SlowLogger:
        def log(self, *args):
            entered.set()
            blocked.wait(5)
    class Store:
        def read_diagnostics(self, cursor, limit):
            return dict(boot_id='boot', next_sequence=cursor+1, overwritten=0,
                        events=[json.dumps(dict(level=20,event='server_ready'))]*100)
    pump = DiagnosticPump(Store(), logger=SlowLogger(), interval=.001, queue_size=10)
    pump.start()
    assert entered.wait(1)
    time.sleep(.05)
    assert pump.dropped > 0
    assert pump.pending <= 10
    began = time.monotonic()
    pump.close(timeout=.05)
    assert time.monotonic()-began < .3
    blocked.set()


def test_pump_restarts_boot_cursor_and_survives_logger_failure():
    calls = []
    recovered = threading.Event()
    class Store:
        def read_diagnostics(self, cursor, limit):
            calls.append(cursor)
            boot = 'a' if len(calls) == 1 else 'b'
            if len(calls) >= 3 and cursor == 0:
                recovered.set()
            return dict(boot_id=boot,next_sequence=4,overwritten=0,
                        events=[json.dumps(dict(level=20,event='server_ready'))])
    class BrokenLogger:
        def log(self, *args):
            raise RuntimeError('do not expose this output failure')
    pump = DiagnosticPump(Store(),logger=BrokenLogger(),interval=.001).start()
    assert recovered.wait(1)
    time.sleep(.02)
    pump.close()
    assert calls[:3] == [0,4,0]
    assert pump.failures > 0


def test_session_connection_events_are_plain_bounded_pump_records():
    from openbq_kdb.session import KdbSession
    from tests.test_session import cfg
    s = KdbSession(cfg(may_spawn=False))
    class Store:
        session = s
        def read_diagnostics(self, cursor, limit):
            return dict(boot_id='a',next_sequence=0,overwritten=0,events=[])
    records = []
    class Logger:
        def log(self, level, fmt, encoded):
            records.append(json.loads(encoded))
    pump = DiagnosticPump(Store(),logger=Logger(),interval=.01).start()
    s.run(lambda: s._diagnostic_event('connection_recovered'))
    for _ in range(100):
        if records:
            break
        time.sleep(.001)
    pump.close()
    s.close()
    assert records[0]['event'] == 'connection_recovered'
    assert records[0]['component'] == 'kdb-client'
    assert records[0]['timestamp'].endswith('Z')
