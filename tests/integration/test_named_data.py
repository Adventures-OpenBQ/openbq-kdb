"""Native authenticated data and accepted chart source qualification."""
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import time

import pytest


@pytest.fixture
def data_server(tmp_path):
    q = os.environ.get('OPENBQ_TEST_Q')
    if not q:
        pytest.skip('OPENBQ_TEST_Q required')
    token = secrets.token_urlsafe(48)
    passwords = tmp_path / 'passwords'
    principals = {'writer': ['recorder', 'cache', 'export'], 'chart': ['chart']}
    principals.update({f'role_{r}': [r] for r in ['query', 'cache', 'recorder', 'export', 'diagnostics', 'chart']})
    passwords.write_text(''.join(f'{u}:{token}\n' for u in principals))
    passwords.chmod(0o600)
    roles = tmp_path / 'roles.json'
    roles.write_text(json.dumps({'schema_version': 1, 'principals': principals}))
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        port = s.getsockname()[1]
    env = dict(os.environ, KDB_AUTH_PASSWORDS_FILE=str(passwords),
               KDB_AUTH_ROLES_FILE=str(roles), QHOME=str(Path(q).parent.parent))
    log = (tmp_path / 'server.log').open('w+')
    command = [sys.executable, '-m', 'openbq_kdb.bootstrap', '--q', q, '--port', str(port),
               '--data-dir', str(tmp_path / 'data')]
    processes = []

    def start(init=None, *, wait=True):
        args = command + (["--init-script", str(init)] if init else [])
        proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=log,
                                stderr=subprocess.STDOUT, env=env)
        processes.append(proc)
        if not wait:
            return proc
        for _ in range(200):
            if proc.poll() is not None:
                log.seek(0)
                pytest.fail(log.read())
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.05):
                    return proc
            except OSError:
                time.sleep(.02)
        log.seek(0)
        pytest.fail(log.read())

    def connect(user='writer'):
        import pykx as kx
        return kx.SyncQConnection('127.0.0.1', port, username=user, password=token,
                                 no_ctx=True, reconnection_attempts=-1, timeout=2)

    connect.port = port
    connect.token = token
    connect.root = tmp_path
    start.processes = processes
    start()
    yield connect, start
    for proc in processes:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)
        proc.stdin.close()
    log.close()


def rpc(conn, op, *args):
    import pykx as kx
    return conn(kx.SymbolAtom('.openbq.rpc'), kx.SymbolAtom(op), kx.List([kx.CharVector(a) if isinstance(a, str) else a for a in args]),
                kx.CharVector('test'), debug=False, skip_debug=True)


def ticks():
    import pandas as pd
    return pd.DataFrame({'time': pd.to_datetime(['2026-11-22T12:00:00.000000001'] * 2),
                         'sym': ['AAPL'] * 2, 'price': [100., 100.], 'size': [2., 2.]})


def test_named_ticks_roles_multiset_and_restart(data_server):
    connect, start = data_server
    with connect() as c:
        assert rpc(c, 'writeTicks', ticks()).py() == 2
        assert rpc(c, 'ticksPresent', ticks()).py() == 2
        assert len(rpc(c, 'readTicks', 'AAPL', 10).pd()) == 2
        with pytest.raises(Exception, match='KDB_ARGUMENT'):
            rpc(c, 'writeTicks', ticks().assign(extra=1))
    with connect('chart') as c:
        with pytest.raises(Exception, match='KDB_AUTHORIZATION'):
            rpc(c, 'writeTicks', ticks())
        assert rpc(c, 'streamStatus').py()['current_cursor']['seq'] == b'2'


def chart_view(mode='ticks', **kwargs):
    return dict(symbol='AAPL', mode=mode, start='2026-11-22T11:59:00.000000000Z',
                max_items=100, **kwargs)


def q_value(v):
    import pykx as kx
    if isinstance(v, str):
        return kx.CharVector(v)
    if isinstance(v, dict):
        return kx.toq({k: q_value(x) for k, x in v.items()})
    return kx.toq(v)


def chart_rpc(c, op, *args):
    from openbq_kdb.protocol import chart_result
    return chart_result(rpc(c, op, *[q_value(v) for v in args]).py())


def test_chart_snapshot_changes_duplicate_identity_and_late_candle(data_server):
    import pandas as pd
    connect, _ = data_server
    with connect() as w, connect('chart') as c:
        rpc(w, 'writeTicks', ticks())
        snap = chart_rpc(c, 'readChartSnapshot', chart_view())
        assert snap['cursor']['seq'] == '2'
        assert [x['tick_id']['seq'] for x in snap['items']] == ['1', '2']
        assert snap['items'][0]['time'] == '2026-11-22T12:00:00.000000001Z'
        cursor = snap['cursor']
        other = ticks().iloc[:1].assign(sym='MSFT')
        rpc(w, 'writeTicks', other)
        changes = chart_rpc(c, 'readChartChanges', cursor, chart_view())
        assert changes.get('through_cursor', changes).get('seq') == '3', changes
        assert changes['batches'] == []
        view = chart_view('candles', interval='1m')
        before = chart_rpc(c, 'readChartSnapshot', view)
        assert before['items'][0]['volume'] == 4.
        late = ticks().iloc[:1].copy()
        late['time'] = pd.to_datetime(['2026-11-22T12:00:00.000000000'])
        late['price'] = 90.
        rpc(w, 'writeTicks', late)
        after = chart_rpc(c, 'readChartChanges', before['cursor'], view)
        assert after['candles'][0]['revision']['seq'] == '4'
        assert after['candles'][0]['open'] == 90.
        assert after['candles'][0]['close'] == 100.
        assert after['candles'][0]['volume'] == 6.


def test_named_cache_export_and_limits(data_server):
    import pandas as pd
    import pykx as kx
    connect, _ = data_server
    lo = kx.TimestampAtom(pd.Timestamp('2026-11-22T00:00:00').to_pydatetime())
    hi = kx.TimestampAtom(pd.Timestamp('2026-11-23T00:00:00').to_pydatetime())
    bars = pd.DataFrame({'t': pd.to_datetime(['2026-11-22T12:00:00']), 'open': [100.]})
    with connect() as c:
        rpc(c, 'writeBars', 'AAPL', '1m', bars)
        assert len(rpc(c, 'readBars', 'AAPL', '1m', lo, hi).pd()) == 1
        rpc(c, 'recordCoverage', 'AAPL', '1m', lo, hi)
        assert len(rpc(c, 'readCoverage', 'AAPL', '1m').pd()) == 1
        rpc(c, 'touch', 'AAPL', '1m')
        assert len(rpc(c, 'lruRows').pd()) == 1
        rpc(c, 'writeSnapshot', 'AAPL', '{"price":100}')
        assert rpc(c, 'readSnapshot', 'AAPL').pd()['payload'].iloc[0] == b'{"price":100}'
        rpc(c, 'writeTicks', ticks())
        assert rpc(c, 'dayCount', lo, hi).py() == 2
        assert list(rpc(c, 'daySlice', lo, hi, 0, 10).pd().columns) == ['time', 'sym', 'price', 'size']
        with pytest.raises(Exception, match='KDB_LIMIT'):
            rpc(c, 'readTicks', 'AAPL', 1001)
        rpc(c, 'drop', 'AAPL', '1m')
        assert rpc(c, 'barPrototype', 'AAPL', '1m').py() == []
        assert rpc(c, 'readCoverage', 'AAPL', '1m').pd().empty


def test_persistence_restart_new_epoch_and_old_cursor_reset(data_server):
    connect, start = data_server
    with connect() as c:
        rpc(c, 'writeTicks', ticks())
    with connect('chart') as c:
        snap = chart_rpc(c, 'readChartSnapshot', chart_view())
    start.processes[-1].terminate()
    start.processes[-1].wait(timeout=5)
    start()
    with connect() as c:
        assert rpc(c, 'ticksPresent', ticks()).py() == 2
    with connect('chart') as c:
        now = chart_rpc(c, 'readChartSnapshot', chart_view())
        assert now['cursor']['epoch'] != snap['cursor']['epoch']
        assert len(now['items']) == 2
        assert chart_rpc(c, 'readChartChanges', snap['cursor'], chart_view())['reason'] == 'epoch_changed'


def test_secured_session_store_uses_named_api_on_owner_thread(data_server):
    connect, _ = data_server
    token_file = connect.root / 'client-token'
    token_file.write_text(connect.token)
    token_file.chmod(0o600)
    script = '''
from datetime import datetime
import pandas as pd
from openbq_kdb.config import KdbConfig
from openbq_kdb.session import KdbSession
from openbq_kdb.store import KdbStore, NamedKdbStore
import os
cfg=KdbConfig(host=None,port=int(os.environ['TEST_PORT']),bind_host='127.0.0.1',
              may_spawn=False,memory_mb=1024,watermark=.75,upstream='eodhd',
              qhome='/not-used',qlic='/not-used',local_qhome='/not-used')
s=KdbSession(cfg)
store=KdbStore(s)
try:
 assert isinstance(store,NamedKdbStore)
 frame=pd.DataFrame(dict(time=pd.to_datetime(['2026-11-22T12:00:00']),
                         sym=['AAPL'],price=[1.],size=[2.]))
 assert store.write_ticks(frame)==1
 assert store.ticks_present(frame)==1
 assert len(store.read_ticks('AAPL',10))==1
 assert store.latest_tick('AAPL')['price']==1.
 assert store.tick_span('AAPL') is not None
 store.write_bars('AAPL','1m',pd.DataFrame(dict(t=pd.to_datetime(['2026-11-22']),open=[1.])))
 assert len(store.read_bars('AAPL','1m',datetime(2026,11,21),datetime(2026,11,23)))==1
 store.write_snapshot('AAPL',{'price':1})
 assert store.read_snapshot('AAPL',100)['price']==1
 assert {'ping','readChartSnapshot','chart_stream_v1'}<=s.capabilities
 assert store.prune_ticks(datetime(2026,11,23))==0
finally:
 s.close()
'''
    result = subprocess.run([sys.executable, '-c', script], capture_output=True, text=True,
                            env=dict(os.environ, KDB_USERNAME='writer',
                                     KDB_PASSWORD_FILE=str(token_file), TEST_PORT=str(connect.port)))
    assert result.returncode == 0, result.stdout + result.stderr


def test_chart_retention_expiry_ahead_and_prune_metadata(data_server):
    import pandas as pd
    import pykx as kx
    connect, start = data_server
    start.processes[-1].terminate()
    start.processes[-1].wait(timeout=5)
    init = connect.root / 'limits.q'
    init.write_text('.chart.maxRows:2;\n')
    start(init)
    with connect() as w, connect('chart') as c:
        initial = chart_rpc(c, 'streamStatus')['current_cursor']
        rpc(w, 'writeTicks', ticks())
        first = chart_rpc(c, 'readChartSnapshot', chart_view())
        rpc(w, 'writeTicks', ticks().iloc[:1])
        assert chart_rpc(c, 'readChartChanges', initial, chart_view())['reason'] == 'cursor_expired'
        future = dict(first['cursor'], seq='99')
        assert chart_rpc(c, 'readChartChanges', future, chart_view())['reason'] == 'cursor_ahead'
        assert chart_rpc(c, 'readChartChanges', first['cursor'], chart_view())['through_cursor']['seq'] == '3'
        cutoff = kx.TimestampAtom(pd.Timestamp('2026-11-22T12:00:00.000000002').to_datetime64())
        assert rpc(w, 'pruneTicks', cutoff).py() == 0
        assert chart_rpc(c, 'readChartChanges', first['cursor'], chart_view())['reason'] == 'coverage_lost'
        new = ticks().iloc[:1].copy()
        new['time'] = pd.to_datetime(['2026-11-22T12:00:01'])
        rpc(w, 'writeTicks', new)
        snap = chart_rpc(c, 'readChartSnapshot', chart_view())
        assert len(snap['items']) == 1
        assert snap['items'][0]['tick_id']['seq'] == '4'


def test_chart_items_match_shared_structural_schema(data_server):
    import jsonschema
    from importlib.resources import files
    connect, _ = data_server
    schema = json.loads(files('openbq_kdb').joinpath('resources/chart-stream-v1.schema.json').read_text())
    with connect() as w, connect('chart') as c:
        rpc(w, 'writeTicks', ticks())
        tick = chart_rpc(c, 'readChartSnapshot', chart_view())['items'][0]
        candle = chart_rpc(c, 'readChartSnapshot', chart_view('candles', interval='1m'))['items'][0]
        jsonschema.validate(tick, {'$ref': '#/$defs/tick', '$defs': schema['$defs']})
        jsonschema.validate(candle, {'$ref': '#/$defs/candle', '$defs': schema['$defs']})


def test_prune_resnapshot_advances_floor_without_permanent_reset(data_server):
    import pandas as pd
    import pykx as kx
    connect, _ = data_server
    view = chart_view('candles', interval='1m')
    with connect() as w, connect('chart') as c:
        rpc(w, 'writeTicks', ticks())
        cutoff = kx.TimestampAtom(pd.Timestamp('2026-11-22T12:00:30').to_datetime64())
        rpc(w, 'pruneTicks', cutoff)
        snapshot = chart_rpc(c, 'readChartSnapshot', view)
        assert snapshot['view_floor']['bucket_start'] == '2026-11-22T12:01:00.000000000Z'
        assert snapshot['items'] == []
        changes = chart_rpc(c, 'readChartChanges', snapshot['cursor'], view)
        assert changes['candles'] == []
        assert changes['unavailable_buckets'][0]['bucket_start'] == '2026-11-22T12:00:00.000000000Z'
        late = ticks().iloc[:1].copy()
        rpc(w, 'writeTicks', late)
        changes = chart_rpc(c, 'readChartChanges', snapshot['cursor'], view)
        assert changes['candles'] == []
        assert changes['unavailable_buckets'][0]['bucket_start'] == '2026-11-22T12:00:00.000000000Z'
        fresh = chart_rpc(c, 'readChartSnapshot', view)
        assert fresh['items'] == []
        late['time'] = pd.to_datetime(['2026-11-22T12:01:10'])
        rpc(w, 'writeTicks', late)
        latest = chart_rpc(c, 'readChartChanges', changes['through_cursor'], view)
        assert latest['candles'][0]['volume'] == 2.


def test_every_implemented_operation_checks_role_before_arguments(data_server):
    from importlib.resources import files
    connect, _ = data_server
    manifest = json.loads(files('openbq_kdb').joinpath('resources/protocol-v1.json').read_text())
    for role, allowed in manifest['roles'].items():
        with connect('role_' + role) as c:
            for operation, spec in manifest['operations'].items():
                if not spec['implemented']:
                    continue
                try:
                    rpc(c, operation)
                except Exception as exc:
                    if operation in allowed:
                        assert 'KDB_AUTHORIZATION' not in str(exc), (role, operation)
                        assert 'KDB_ARGUMENT' in str(exc), (role, operation, str(exc))
                    else:
                        assert 'KDB_AUTHORIZATION' in str(exc), (role, operation, str(exc))
                else:
                    assert operation in allowed, (role, operation)


def test_stream_apply_failure_exits_and_recovers_durable_batch(data_server):
    connect, start = data_server
    start.processes[-1].terminate()
    start.processes[-1].wait(timeout=5)
    init = connect.root / 'fail-stream.q'
    init.write_text('.chart.accept:{[d] \'"allocation-test"};\n')
    failing = start(init)
    with connect() as c:
        with pytest.raises(Exception):
            rpc(c, 'writeTicks', ticks())
    assert failing.wait(timeout=5) != 0
    start()
    with connect() as c:
        assert rpc(c, 'ticksPresent', ticks()).py() == 2
    with connect('chart') as c:
        assert len(chart_rpc(c, 'readChartSnapshot', chart_view())['items']) == 2


def test_empty_nullable_and_oversized_ticks_preserve_atomicity(data_server):
    import pandas as pd
    connect, _ = data_server
    with connect() as w, connect('chart') as c:
        assert rpc(w, 'writeTicks', ticks().iloc[:0]).py() == 0
        assert chart_rpc(c, 'streamStatus')['current_cursor']['seq'] == '0'
        nullable = ticks().iloc[:1].assign(size=float('nan'))
        assert rpc(w, 'writeTicks', nullable).py() == 1
        assert rpc(w, 'ticksPresent', nullable).py() == 1
        assert chart_rpc(c, 'readChartSnapshot', chart_view())['items'][0]['size'] is None
        huge = pd.concat([ticks().iloc[:1]] * 100001, ignore_index=True)
        with pytest.raises(Exception, match='KDB_LIMIT'):
            rpc(w, 'writeTicks', huge)
        assert chart_rpc(c, 'streamStatus')['current_cursor']['seq'] == '1'


def test_snapshot_bound_floor_batches_and_catchup_limit(data_server):
    connect, start = data_server
    start.processes[-1].terminate()
    start.processes[-1].wait(timeout=5)
    init = connect.root / 'scan-limit.q'
    init.write_text('.chart.scan:3;\n')
    start(init)
    with connect() as w, connect('chart') as c:
        before = chart_rpc(c, 'streamStatus')['current_cursor']
        rpc(w, 'writeTicks', ticks())
        rpc(w, 'writeTicks', ticks().iloc[:1])
        changes = chart_rpc(c, 'readChartChanges', before, chart_view())
        assert [len(b['ticks']) for b in changes['batches']] == [2, 1]
        assert changes['batches'][0]['batch_id'] != changes['batches'][1]['batch_id']
        view = dict(chart_view(), max_items=1)
        snapshot = chart_rpc(c, 'readChartSnapshot', view)
        assert snapshot['truncated'] is True
        assert snapshot['view_floor']['seq'] == '3'
        assert len(snapshot['items']) == 1
        rpc(w, 'writeTicks', ticks().iloc[:1])
        assert chart_rpc(c, 'readChartChanges', before, chart_view())['reason'] == 'catchup_limit'


def test_qualified_client_classifies_auth_and_operation_policy_errors(data_server):
    from openbq_kdb.credentials import ClientCredentials
    from openbq_kdb.protocol import (KdbAuthenticationError, KdbAuthorizationError,
                                     KdbArgumentError, connect_qualified, named_call)
    connect, _ = data_server
    with pytest.raises(KdbAuthenticationError, match='KDB_AUTHENTICATION'):
        connect_qualified('127.0.0.1', connect.port, ClientCredentials('chart', 'wrong'))
    with connect_qualified('127.0.0.1', connect.port,
                           ClientCredentials('chart', connect.token)) as c:
        with pytest.raises(KdbAuthorizationError, match='KDB_AUTHORIZATION'):
            named_call(c, 'writeTicks', ticks())
        with pytest.raises(KdbArgumentError, match='KDB_ARGUMENT'):
            named_call(c, 'readChartSnapshot', {'query': '1+1'})


def test_midbucket_start_floor_keeps_retained_candle_revisions(data_server):
    import pandas as pd
    connect, _ = data_server
    view = dict(chart_view('candles', interval='1m'), start='2026-11-22T12:00:30.000000000Z')
    with connect() as w, connect('chart') as c:
        frame = ticks().iloc[:1].copy()
        frame['time'] = pd.to_datetime(['2026-11-22T12:00:31'])
        rpc(w, 'writeTicks', frame)
        snap = chart_rpc(c, 'readChartSnapshot', view)
        assert snap['view_floor']['bucket_start'] == snap['items'][0]['key']['bucket_start']
        frame['time'] = pd.to_datetime(['2026-11-22T12:00:30'])
        frame['price'] = 90.
        rpc(w, 'writeTicks', frame)
        changes = chart_rpc(c, 'readChartChanges', snap['cursor'], view)
        assert changes['candles'][0]['key']['bucket_start'] == snap['view_floor']['bucket_start']
        assert changes['candles'][0]['open'] == 90.


def test_diagnostics_role_utc_ring_and_restart_cursor(data_server):
    connect, _ = data_server
    with connect('role_diagnostics') as c:
        result = chart_rpc(c, 'readDiagnostics', 0, 100)
        assert result['boot_id']
        records = [json.loads(event) for event in result['events']]
        assert any(event['event'] == 'server_ready' for event in records)
        for event in records:
            assert event['schema_version'] == 1
            assert event['timestamp'].endswith('Z')
            assert 'password' not in event
    with connect('chart') as c:
        with pytest.raises(Exception, match='KDB_AUTHORIZATION'):
            rpc(c, 'readDiagnostics', 0, 100)


def diagnostic_events(c, after=0):
    result = chart_rpc(c, 'readDiagnostics', after, 100)
    return result, [json.loads(raw) for raw in result['events']]


def test_diagnostic_flood_rate_limit_and_secret_field_whitelist(data_server):
    connect, start = data_server
    with connect('chart') as c:
        for _ in range(80):
            with pytest.raises(Exception, match='KDB_AUTHORIZATION'):
                rpc(c, 'writeTicks', ticks())
    with connect('role_diagnostics') as c:
        result, events = diagnostic_events(c)
        denied = [e for e in events if e['event'] == 'auth_denied']
        assert denied
        from collections import Counter
        assert max(Counter(e['timestamp'][:19] for e in denied).values()) <= 10
        assert all('price' not in e and 'password' not in e for e in events)
    start.processes[-1].terminate()
    start.processes[-1].wait(timeout=5)
    init = connect.root / 'flood.q'
    init.write_text('''
{[i] .diag.counts:.diag.vocabulary!count[.diag.vocabulary]#0j;
 .openbq.log[`info;`server_ready;`schema_version`timestamp`event`level`password`payload`request_id!(999;"secret-time";`evil;0;"secret-token";"secret-payload";"safe-id")]} each til 1100;
if[count .finos.structlog.structlogTable;'"default-table-grew"];
''')
    start(init)
    with connect('role_diagnostics') as c:
        result, events = diagnostic_events(c)
        assert result['overwritten'] >= 100
        assert len(events) == 100
        assert all(len(raw.encode()) <= 4096 for raw in result['events'])
        assert all(e['schema_version'] == 1 and e['event'] == 'server_ready' for e in events)
        assert 'secret-' not in json.dumps(events)
        assert all(e['timestamp'].endswith('Z') and len(e['timestamp']) == 30 for e in events)
        assert result['boot_id']


def test_logger_failure_preserves_ticklog_failure_and_restart_boot(data_server):
    connect, start = data_server
    with connect('role_diagnostics') as c:
        old, _ = diagnostic_events(c)
    start.processes[-1].terminate()
    start.processes[-1].wait(timeout=5)
    init = connect.root / 'broken-logger.q'
    init.write_text('''
.diag.logger:`info`debug`warning`error!4#{[a;b] '"logger-failed"};
.tlog.append:{[d] '"tlog: append-test"};
''')
    start(init)
    with connect() as c:
        with pytest.raises(Exception,match='tlog: append-test'):
            rpc(c,'writeTicks',ticks())
        assert rpc(c,'ticksPresent',ticks()).py() == 0
    with connect('role_diagnostics') as c:
        now, _ = diagnostic_events(c,old['next_sequence'])
        assert now['boot_id'] != old['boot_id']


def test_diagnostic_suppression_flush_is_observable_and_bounded(data_server):
    connect, _ = data_server
    with connect('chart') as c:
        for _ in range(30):
            with pytest.raises(Exception,match='KDB_AUTHORIZATION'):
                rpc(c,'memory')
    time.sleep(1.05)
    with connect('role_diagnostics') as c:
        _, events = diagnostic_events(c)
        suppressed = [e for e in events if e['event'] == 'log_events_suppressed']
        assert suppressed and sum(e['rows'] for e in suppressed) > 0
        with pytest.raises(Exception,match='KDB_LIMIT'):
            rpc(c,'readDiagnostics',0,101)


def test_unrecoverable_whole_log_replay_error_refuses_readiness(data_server):
    connect, start = data_server
    with connect() as c:
        rpc(c,'writeTicks',ticks())
    start.processes[-1].terminate()
    start.processes[-1].wait(timeout=5)
    logfile = next((connect.root/'data'/'logs').glob('tlog_*'))
    seed = connect.root/'append-bad-log.q'
    seed.write_text('h:hopen hsym `$getenv `OPENBQ_BAD_LOG;h enlist (`upd;`trades;42);hclose h;exit 0;\n')
    q = os.environ['OPENBQ_TEST_Q']
    result = subprocess.run([q,str(seed),'-q'],stdin=subprocess.DEVNULL,capture_output=True,
                            env=dict(os.environ,QHOME=str(Path(q).parent.parent),OPENBQ_BAD_LOG=str(logfile)))
    assert result.returncode == 0
    proc = start(wait=False)
    try:
        assert proc.wait(timeout=3) != 0
    except subprocess.TimeoutExpired:
        pytest.fail('unrecoverable valid-chunk replay error admitted a ready server')
    with pytest.raises(OSError):
        socket.create_connection(('127.0.0.1',connect.port),timeout=.1)


def test_torn_tail_still_repairs_valid_prefix_without_duplicates(data_server):
    connect,start=data_server
    with connect() as c:
        rpc(c,'writeTicks',ticks())
    start.processes[-1].terminate()
    start.processes[-1].wait(timeout=5)
    logfile=next((connect.root/'data'/'logs').glob('tlog_*'))
    with logfile.open('ab') as output:
        output.write(b'\x01\x00\x00')
    start()
    with connect() as c:
        assert rpc(c,'ticksPresent',ticks()).py()==2
        assert len(rpc(c,'readTicks','AAPL',10).pd())==2
    with connect('role_diagnostics') as c:
        _,events=diagnostic_events(c)
        assert any(e.get('error_code')=='TLOG_TORN' for e in events)


def test_bootstrap_discards_raw_runtime_and_license_output(data_server):
    connect,start=data_server
    start.processes[-1].terminate()
    start.processes[-1].wait(timeout=5)
    init=connect.root/'noisy-init.q'
    init.write_text('-1 "secret-license-output-DO-NOT-EXPOSE";\n')
    start(init)
    with connect() as c:
        assert rpc(c,'writeTicks',ticks()).py()==2
    assert 'secret-license-output' not in (connect.root/'server.log').read_text()
