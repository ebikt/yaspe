#!/usr/bin/python3
try:
    import aiopg
except ImportError:
    pass
else:
    # This import should be at the top of the file because we need to apply monkey patch
    # before executing any other code.
    # We want to revert this change: https://github.com/python/cpython/pull/1030
    # Additional context is here: https://github.com/aio-libs/aiopg/issues/837
    import selectors  # isort:skip # noqa: F401

    selectors._PollLikeSelector.modify = (  # type: ignore
        selectors._BaseSelectorImpl.modify  # type: ignore
    )  # noqa: E402

import asyncio, aiohttp.web
from aio_exporter import Exporter, Metric, BaseMetric
import os
import sys
import yaml
import argparse
import inspect
import subprocess
import time
import traceback

from typing import Optional, List, Dict, Union, Callable, Tuple, cast, Iterator, Awaitable, Set, NoReturn, Type, TypeVar, Generic, Any, Sequence

DEFAULT_CFG=os.path.join(os.path.dirname(os.path.realpath(__file__)),'config.yml')

MYPY=False

# {{{ DBTyping
def X() -> NoReturn:
    raise NotImplementedError()

T = TypeVar('T')
class AsyncContext(Generic[T]):
    async def __aenter__(self) -> T: X()
    async def __aexit__(self, t:Type[BaseException], e:BaseException, tb:Any) -> None: X()

class DBTyping:
    Value = Union[int, str, float, bool, None]
    class Pool:
        def acquire(self) -> 'DBTyping.AConnection': X()
        def close(self) -> None: X()
        async def wait_closed(self) -> None: X()
    class AConnection(AsyncContext['DBTyping.ConnContext']): pass
    class ConnContext:
        def cursor(self) -> 'DBTyping.ACursor': X()
    class ACursor(AsyncContext['DBTyping.CursorContext']): pass
    class CursorContext:
        description: Sequence[Tuple[str, int, int, int, Optional[int], Optional[int], None]]
        rowcount: int
        rownumber: int
        arraysize: int
        async def execute(self, query:str) -> None: X()
        async def fetchall(self) -> List[Sequence['DBTyping.Value']]: X()
# }}}

# {{{ Yaml typing 
class Unspec:
    pass

class Special:
    def __init__(self, t:str):
        assert t in ('zero', 'error')
        self.is_zero  = t == 'zero'
        self.is_error = t == 'error'
    ZERO:'Special'
    ERROR:'Special'
Special.ZERO   = Special('zero')
Special.ERROR  = Special('error')

class YamlContext: # {{{
    def __init__(self, context:str) -> None:
        self.context = context
    def error(self, msg:str, exc:Callable[[str], Exception]=ValueError) -> NoReturn:
        if self.context:
            raise exc(f"{self.context}: {msg}")
        else:
            raise exc(f"{msg}")

    def fatal(self, msg:str) -> NoReturn:
        self.error(msg, AssertionError)
# }}}

class YamlValue(YamlContext): # {{{ 
    def __init__(self, value:Unspec, context: str) -> None:
        self.value = value
        super().__init__(context)

    def maybe(self, t:Type[T], zero:Union[T,Special,None] = Special.ZERO, none:Union[T,Special,None] = None) -> Optional[T]:
        if self.value is None:
            if isinstance(none, Special):
                if none.is_zero:
                    return t()
                if none.is_error:
                    self.error(f"Cannot convert None to {t.__name__}")
                raise AssertionError("Never happens")
            return none
        ret = t(self.value) # type: ignore[call-arg]
        if ret == t():
            if isinstance(zero, Special):
                if zero.is_error:
                    try:
                        if len(ret) != 0: # type: ignore[arg-type]
                            raise Exception()
                    except Exception:
                        self.error(f"Zero value is not allowed.")
                    else:
                        self.error(f"Empty value is not allowed.")
                if not zero.is_zero:
                    raise AssertionError("Never happens")
            else:
                return zero
        return ret

    def to(self, t:Type[T], zero:Union[T,Special,None] = Special.ZERO, none:Union[T,Special,None] = None) -> T:
        if zero is None:
            zero = Special.ERROR
        if none is None:
            none = Special.ERROR
        ret = self.maybe(t, zero, none)
        assert ret is not None
        return ret

    def to_validated_str(self, predicate:Callable[[str],bool], errormsg:str, default:Optional[str] = None) -> str:
        ret = self.maybe(str)
        if ret is None or not predicate(ret):
            if ret in (None, '') and default is not None:
                return default
            self.error(errormsg)
        return ret

    def choice(self, choices:Dict[str, T], errormsg:str, default:Optional[T] = None) -> T:
        c = self.to_validated_str( lambda x: x in choices, errormsg, "__!DEFAULT!" if default is not None else None )
        try:
            return choices[c]
        except KeyError:
            if default is not None:
                return default
            else:
                raise
    def to_dict(self, empty:bool = True, none:bool = False) -> 'YamlDict':
        if self.value is None:
            if not none:
                self.error("Cannot convert None to dict")
            else:
                return YamlDict({}, self.context)
        assert isinstance(self.value, dict)
        if len(self.value) == 0 and not empty:
            self.error("Empty dict not allowed")
        return YamlDict(cast(Dict[str, Unspec], self.value), self.context)

    def to_list(self, empty:bool = True, none:bool = False) -> 'YamlList':
        if self.value is None:
            if not none:
                self.error("Cannot convert None to list")
            else:
                return YamlList([], self.context)
        assert isinstance(self.value, list)
        if len(self.value) == 0 and not empty:
            self.error("Empty list not allowed")
        return YamlList(cast(List[Unspec], self.value), self.context)
# }}}

class YamlDict(YamlContext): # {{{
    def __init__(self, value:Dict[str, Unspec], context: str) -> None:
        self.value = value
        self.unseen = set(value.keys())
        super().__init__(context)

    def __getitem__(self, key:str) -> YamlValue:
        self.unseen.discard(key)
        return YamlValue(self.value[key], f"{self.context}.{key}")

    def __iter__(self) -> Iterator[str]:
        return iter(self.value)

    def __contains__(self, key:str) -> bool:
        return key in self.value

    def __len__(self) -> int:
        return len(self.value)

    def get(self, key:str, default:Union[str, int, None, Unspec] = None) -> YamlValue:
        if key not in self.value:
            return YamlValue(cast(Unspec, default), f"{self.context}.{key}!")
        return self[key]

    def keys(self) -> Iterator[str]:
        return iter(self.value)

    def items(self) -> Iterator[Tuple[str,YamlValue]]:
        for k, v in self.value.items():
            self.unseen.discard(k)
            yield k, YamlValue(v, f"{self.context}.{k}")

    def values(self) -> Iterator[YamlValue]:
        for k, v in self.items():
            yield v

    def show_unseen(self, die:bool = False) -> None:
        if self.unseen:
            if self.context:
                prefix = f'Warning: {self.context}:'
            else:
                prefix = 'Warning:'
            print(f'{prefix} unknown configuration keys: {", ".join(sorted(self.unseen))}', file=sys.stderr)
            if die:
                sys.exit(1)
# }}}

class YamlList(YamlContext): # {{{
    def __init__(self, value:List[Unspec], context: str) -> None:
        self.value = value
        super().__init__(context)

    def __getitem__(self, key:int) -> YamlValue:
        try:
            return YamlValue(self.value[key], f"{self.context}[{key}]")
        except LookupError as e:
            self.error(str(e), type(e))

    def __iter__(self) -> Iterator[YamlValue]:
        for i, v in enumerate(self.value):
            yield YamlValue(v, f"{self.context}[{i}]")
        return iter(self.value)

    def __len__(self) -> int:
        return len(self.value)
# }}}
# }}}

class DBConnection: # {{{
    id: str
    pool: DBTyping.Pool

    ARG_TYPES:Dict[str, Tuple[object, ...]]
    async def get_pool_impl(self, kwargs:Dict[str, Union[str, int, float, bool]], maxsize:int = 10, minsize:int = 0) -> DBTyping.Pool:
        raise NotImplementedError()

    def __init__(self, name: str, cfg:YamlDict) -> None:
        self.id   = name
        self.kwargs:Dict[str, Union[str, int, float, bool, None]] = {}
        self.max_connections = cfg.get('max_connections').to(int, 10, 10)
        for tname, targs in self.ARG_TYPES.items():
            self.kwargs[tname] = cfg.get(tname).maybe(*targs) # type: ignore[assignment, arg-type]

    async def get_pool(self) -> DBTyping.Pool:
        kwargs = { k: v for k, v in self.kwargs.items() if v is not None }
        return await self.get_pool_impl(kwargs, maxsize = self.max_connections, minsize = 0)

    async def execute(self, query:str) -> Tuple[Sequence[str], List[Sequence[DBTyping.Value]]]:
        if not hasattr(self, 'pool'):
            self.pool = await self.get_pool()
        async with self.pool.acquire() as conn: 
            async with conn.cursor() as cur:
                await cur.execute(query)
                columns = [ x[0] for x in cur.description ]
                rows = await cur.fetchall()
                return columns, rows
# }}}

class MysqlDBConnection(DBConnection): # {{{

    ARG_TYPES = {
        'host': (str, None),
        'user': (str, None),
        'password': (str,),
        'db': (str, None),
        'port': (int, None),
        'unix_socket': (str, None),
        'charset': (str, None),
        #'sql_mode',
        'read_default_file': (str, None),
        #'conv',
        'use_unicode': (bool,), #FIXME: proper boolean parsing
        #'client_flag',
        #'cursorclass',
        'init_command': (str, None),
        'connect_timeout': (int, None),
        #'ssl',
        'auth_plugin': (str, None),
        'program_name': (str, None),
        'server_public_key': (str, None),
    }

    async def get_pool_impl(self, kwargs:Dict[str, Union[str, int, float, bool]], maxsize:int = 10, minsize:int = 0) -> DBTyping.Pool:
        if not MYPY:
            import aiomysql
            return await aiomysql.create_pool(maxsize = maxsize, minsize = minsize, **kwargs)
        else:
            X()
# }}}

class PostgresqlDBConnection(DBConnection): # {{{

    ARG_TYPES = {
        'dsn': (str, None),
        'dbname': (str, None),
        'user': (str, None),
        'password': (str, None),
        'host': (str, None),
        'port': (str, None),
    }

    async def get_pool_impl(self, kwargs:Dict[str, Union[str, int, float, bool]], maxsize:int = 10, minsize:int = 0) -> DBTyping.Pool:
        dsn = kwargs.pop('dsn', None)
        assert dsn is None or isinstance(dsn, str)
        import aiopg
        pool = await aiopg.create_pool(dsn, maxsize = maxsize, minsize = minsize, **kwargs) # type: ignore [arg-type]
        return cast(DBTyping.Pool, pool)
# }}}

class Seq: # {{{
    _v = 1
    def __call__(self) -> int:
        self._v += 1
        return self._v
TICK = Seq()
# }}}

class CfgMetric: # {{{
    TYPES = {x:x for x in ('gauge', 'counter', 'untyped')}

    def __init__(self, name:str, cfg: YamlValue) -> None:
        self.id = name
        cd = cfg.to_dict()
        self.help = cd.get('help').to(str, zero=None)
        t = cd.get('type')
        self.type = t.choice(self.TYPES, 'Invalid metric type')
        dd = cd.get('dimensions').to_list(none=True)
        self.dimensions = [ d.to(str, zero=None) for d in dd ]

    def __call__(self) -> Metric:
        return Metric(self.id, self.type, self.help, *self.dimensions)
# }}}

class EndpointValue: # {{{
    column:           str
    metric:           CfgMetric
    static_labels:    Dict[str, str]
    value_labels:     Dict[str, str]
    at:               Optional[str] = None
    def __init__ (self, column: str, cfgv: YamlValue, metrics: Dict[str, CfgMetric]):
        cfg = cfgv.to_dict()
        self.column = column
        self.metric = cfg.get('metric').choice(metrics, "Referenced metric not found")
        self.static_labels = { k:v.to(str, '','') for k, v in cfg.get('static_labels').to_dict(none=True).items() }
        self.value_labels = { v.to(str,None,None):v.to(str,None,None) for v in cfg.get('value_labels').to_list(none=True) }
        self.at = cfg.get('at').maybe(str,None)
# }}}

class TranslatedValue: # {{{
    column:       int
    metric:       Metric
    dimensions:   List[Union[int, str]]
    value_labels: Dict[str, Union[int, str]]
    at:           Optional[int]

    def __init__(self, columns:Dict[str, int], ev:EndpointValue, metrics:Dict[str,Metric]) -> None:
        self.metric = metrics[ev.metric.id]
        self.column = columns[ev.column]
        if ev.at is not None:
            self.at = columns[ev.at]
        else:
            self.at = None
        self.dimensions = []
        self.value_labels = {}
        self.value_labels.update(ev.static_labels)
        for d in ev.metric.dimensions:
            try:
                static = ev.static_labels[d]
            except KeyError:
                self.dimensions.append(columns[d])
            else:
                assert isinstance(static, str)
                self.value_labels.pop(d)
                self.dimensions.append(static)
        for k, v in ev.value_labels.items():
            if v not in ev.metric.dimensions and v not in self.value_labels:
                self.value_labels[v] = columns[k]

    def collect(self, row:Sequence[DBTyping.Value]) -> None:
        dims:List[str] = []
        for d in self.dimensions:
            if isinstance(d, int):
                dims.append(str(row[d]))
            else:
                dims.append(d)
        adds:Dict[str, str] = {}
        for k, v in self.value_labels.items():
            if isinstance(v, int):
                adds[k] = str(row[v])
            else:
                adds[k] = v
        val = row[self.column]
        if val is None:
            val = -1
        if self.at is None:
            at = None
        else:
            raw_at = row[self.at]
            if raw_at is None:
                at = None
            else:
                #FIXME support various datetime values!
                at = int(raw_at)

        assert isinstance(val, (int, float))
        self.metric.collect(val, *dims, additional_labels=adds, collected_at_ms=at)
# }}}

class EndpointJob: # {{{ 
    connections: List[DBConnection]
    conn_label: Optional[str]
    query: str
    values: List[EndpointValue]

    def __init__(self, cfgv: YamlValue, conns: Dict[str, DBConnection], metrics: Dict[str, CfgMetric]) -> None:
        cfg = cfgv.to_dict()
        c1 = cfg.get('connection')
        cs = cfg.get('connections').to_list(none=True)
        if c1.maybe(str, None) is not None:
            if len(cs):
                cfgv.error("Specify either `connection` or `connections`")
            self.connections = [ c1.choice(conns, f"Connection {c1.maybe(str)} not found") ]
        else:
            self.connections = [
                x.choice(conns, f"Connection {x.maybe(str)} not found")
                for x in cs
            ]
        if len(self.connections) < 1:
            cfgv.error("Specify either `connection` or `connections`")
        self.query  = cfg.get('query').to(str,zero=None)

        check_dims = len(self.connections) > 1
        self.values:List[EndpointValue] = []
        for k, v in cfg.get('values').to_dict(empty=False).items():
            ev = EndpointValue(k, v, metrics)
            if check_dims and self.conn_label not in ev.metric.dimensions:
                v.error(f"Metric {ev.metric.id} does not contain dimension {self.conn_label}, which is required for multi-connection endpoints")
            self.values.append(ev)
# }}}

class Endpoint: # {{{
    def __init__(self, name: str, cfgv: YamlValue, conns: Dict[str,DBConnection], metrics: Dict[str,CfgMetric]) -> None:
        self.id = name
        self.jobs = [
            EndpointJob(v, conns, metrics)
            for v in cfgv.to_list()
        ]
# }}}

class Config: # {{{
    listen: Optional[str]
    timeout: int
    collect_timeout: Optional[int]
    connections: Dict[str, DBConnection]
    endpoints: Dict[str, Endpoint]

    def __init__(self, filename:str) -> None:
        with open(filename) as f:
            root = YamlValue(cast(Unspec, yaml.safe_load(f)), 'CFG').to_dict()

        self.listen          = root.get('listen').maybe(str, None)
        self.timeout         = root.get('timeout').to(int, 30, 30)
        self.collect_timeout = root.get('collect_timeout').maybe(int, None)
        self.metrics         = { k: CfgMetric(k, v) for k, v in root.get('metrics').to_dict(empty=False).items() }

        self.connections = {}
        self.add_static_connections(root.get('connections').to_dict(none=True))
        self.add_ucw_connections(root.get('connections_ucw').to_list(none=True))
        self.endpoints = {
            name: Endpoint(name, cfg, self.connections, self.metrics)
            for name, cfg in root.get('endpoints').to_dict(empty=False).items()
        }
        root.show_unseen()

    DBs = {
        'mysql': MysqlDBConnection,
        'pgsql': PostgresqlDBConnection,
    }

    def add_static_connections(self, connections:YamlDict) -> None:
        for name, cfgv in connections.items():
            if name in self.connections:
                raise ValueError("Duplicit connection {name} found in configuration.")
            cfg = cfgv.to_dict()
            t = cfg.get('type').choice(self.DBs, "Invalid db type")
            self.connections[name] = t(name, cfg)
            cfg.show_unseen(die=True)

    def add_ucw_connections(self, cfgs:YamlList) -> None:
        for cfgv in cfgs:
            cfg = cfgv.to_dict()
            command = cfg.get('command').to_list(empty=False)
            cmdline = [ x.to(str) for x in command ]
            static_fields = { k:v.to(str) for k, v in cfg.get('static_fields').to_dict(none=True).items() }
            dynamic_fields = { k:v.to(str) for k, v in cfg.get('dynamic_fields').to_dict(empty=False).items() }

            parsed_cfg:Dict[str, Dict[str, str]] = {}
            for line in subprocess.run(cmdline, check=True, stdout=subprocess.PIPE, encoding='utf-8').stdout.split('\n'):
                try:
                    line = line.rstrip('\r')
                    if not line.strip(): continue
                    key, value = line.split('=',1)
                    if key[-1:] == ']':
                        key, arrayid = key.split('[',1)
                        arrayid = '[' + arrayid
                    else:
                        arrayid = ''
                    if arrayid not in parsed_cfg:
                        parsed_cfg[arrayid] = {}
                    if value[:1] == value[-1:] == "'":
                        v = value[1:-1]
                        if "'" in v:
                            raise ValueError("Value contains \"'\".") # Not supported yet
                    else:
                        raise ValueError("Value not quoted using \"'\".") # UCW always quotes using "'"
                    parsed_cfg[arrayid][key] = v
                except Exception as e:
                    raise ValueError(f"Error when parsing ucw config line \"{line}\": {e}")
            for ucwk, ucwvars in parsed_cfg.items():
                d:Dict[str,str] = {}
                d.update(static_fields)
                for k, v in dynamic_fields.items():
                    if v in ucwvars:
                        d[k] = ucwvars[v]
                cfg = YamlDict(cast(Dict[str,Unspec],d), f"UCW{ucwk}")
                name = cfg.get('connection').to(str)
                if name in self.connections:
                    raise ValueError("Duplicit connection {name} found in configuration")
                t = cfg.get('type').choice(self.DBs, "Invalid db type")
                self.connections[name] = t(name, cfg)
                cfg.show_unseen(die=True)

    @staticmethod
    def parse_address(s:str) -> Union[Tuple[str, int], str]:
        """ Parse TCP or UNIX address """
        if s.lower().startswith('unix:'):
            return s[5:]
        elif s[:1] in ('@', '/'):
            return s[5:]
        elif s.lower().startswith('tcp:'):
            s = s[4:]
        host, ports = s.rsplit(':', 1)
        if host.startswith('[') and host.endswith(']'):
            host = host[1:-1]
        port = int(ports)
        assert 0 < port < 65536
        return (host, port)
# }}}

class EndpointTask: # {{{
    completed:bool = False
    seq: int
    error: Optional[str]

    def __init__(self, e: Endpoint, j: EndpointJob, c: DBConnection, m:Dict[str, Metric]) -> None:
        self.endpoint = e
        self.job      = j
        self.db       = c
        self.m        = m

    async def run(self, timeout:int) -> Optional[str]:
        try:
            self.result = await asyncio.wait_for(self.db.execute(self.job.query), timeout=timeout)
            self.error = None
        except TimeoutError:
            self.error = 'timeout'
        except Exception as e:
            traceback.print_exc()
            self.error = f'{type(e).__name__}:{e}'
        self.seq = TICK()
        if self.error is not None:
            return self.error
        cols, rows = await self.db.execute(self.job.query)

        for row in rows:
            if len(row) != len(cols): raise Exception("Row length mismatch!")
        cold = { v:i for i, v in enumerate(cols) }

        tvs = [ TranslatedValue(cold, ev, self.m) for ev in self.job.values ]
        for row in rows:
            for tv in tvs:
                tv.collect(row)
        self.completed = True
        return None
# }}}

class SqlExporter(Exporter): # {{{
    def __init__(self) -> None:
        parser = argparse.ArgumentParser(description='Yet Another SQL Prometheus Exporter')
        parser.add_argument('--listen', '-l', default=None, help='Address to listen on')
        parser.add_argument('--config', '-c', default=DEFAULT_CFG, help=f'Configuration file (default {DEFAULT_CFG})')
        parser.add_argument('--test-run', '-t', default=False, action='store_true', help=f'test configuration (runs all endpoints)')
        args = parser.parse_args()
        self.test_run = cast(bool, args.test_run)
        self.config = Config(cast(str, args.config))
        if cast(str, args.listen):
            listen = self.config.parse_address(cast(str, args.listen))
        elif self.config.listen:
            listen = self.config.parse_address(self.config.listen)
        else:
            listen = "127.0.0.1:7999"
        if isinstance(listen, tuple):
            self.host = listen[0]
            self.port = listen[1]
            self.path = None
        else:
            self.host = None
            self.port = None
            self.path = listen
        self.timeout = self.config.timeout
        ct = self.config.collect_timeout
        self.collect_timeout = ct if ct else self.timeout + 3
        super().__init__()

    def get_pages(self) -> Iterator[Tuple[str, Callable[[aiohttp.web.Request], Awaitable[aiohttp.web.Response]], str]]:
        for k, f, h in super().get_pages():
            if k == '/':
                yield k, f, h
            elif k == '/metrics':
                yield k, f, 'Query-parameter endpoint, use /metrics?endpoint=endpoint_name to collect endpoint'
        for ename, endpoint in self.config.endpoints.items():
            if ename:
                metric_names:Set[str] = set()
                for ej in endpoint.jobs:
                    for ev in ej.values:
                        metric_names.add(ev.metric.id)
                helpstr = f'Collect metrics {", ".join(sorted(metric_names))}'
                yield ('/' + ename, self.webpage_metrics, helpstr)

    async def collect_metrics(self, request:aiohttp.web.Request) -> List[BaseMetric]:
        if request.path[:1] != '/':
            raise aiohttp.web.HTTPNotFound(text="Invalid endpoint")
        if request.path != '/metrics':
            endpoints = [ request.path[1:] ]
        else:
            endpoints = request.query.getall('endpoint', [])
        if len(endpoints) < 1:
            raise aiohttp.web.HTTPNotFound(text="Please specify an endpoint")


        tasks:List[EndpointTask] = []
        metrics = { k: m() for k, m in self.config.metrics.items() }
        metrics['+'] =  Metric('sql_exporter_endpoint_queries_ok',  'gauge', 'Number of successfully executed queries', 'endpoint')
        metrics['-'] =  Metric('sql_exporter_endpoint_queries_err',  'gauge', 'Number of successfully executed queries', 'endpoint')

        for eid in endpoints:
            try:
                e = self.config.endpoints[eid]
            except KeyError:
                raise aiohttp.web.HTTPNotFound(text=f"Endpoint {eid} not found")
            for j in e.jobs:
                for c in j.connections:
                    tasks.append( EndpointTask(e, j, c, metrics) )

        status = await cast('asyncio.Future[List[Union[BaseException, Optional[str]]]]',
                            asyncio.gather( *[ t.run(self.timeout) for t in tasks ], return_exceptions=True))

        for i, t in enumerate(tasks):
            if isinstance(status[i], BaseException):
                ex = cast(BaseException, status[i])
                print(f"Uncaught exception:{type(ex)}:{ex}")
                traceback.print_tb(ex.__traceback__)
                t.error = f"{type(ex).__name__}:{ex}"
                t.seq = TICK()

        ok = { eid:0 for eid in endpoints }
        err:Dict[str,List[Tuple[int, str]]] = { eid:[] for eid in endpoints }
        for t in tasks:
            if t.error is None and not t.completed:
                t.error = 'lost'
                t.seq = TICK()
            if t.error is None:
                ok[t.endpoint.id] += 1
            else:
                err[t.endpoint.id].append( (t.seq, t.error) )

        for eid in endpoints:
            metrics['+'].collect(ok[eid], eid)
            if len(err[eid]) == 0:
                exc = '-'
            else:
                exc = min(err[eid])[1]
            metrics['-'].collect(len(err[eid]), eid, additional_labels={'first_error':exc})

        return list(metrics.values())

    async def test(self) -> int:
        class FakeRequest:
            def __init__(self, path:str) -> None:
                self.path = '/' + path
        statistics = { 'sql_exporter_endpoint_queries_ok':0, 'sql_exporter_endpoint_queries_err': 0 }
        for endpoint in sorted(self.config.endpoints.keys()):
            req = cast(aiohttp.web.Request, FakeRequest(endpoint))
            metrics = await self.collect_metrics(req)
            print(f"# ===== {endpoint} =====")
            print(self.render(metrics))
            for m in metrics:
                if m.id in statistics:
                    statistics[m.id] += int(sum([ v.value for v in m.values.values() ]))

        for db in self.config.connections.values():
            if hasattr(db, 'pool'):
                db.pool.close()
        for db in self.config.connections.values():
            if hasattr(db, 'pool'):
                await db.pool.wait_closed()

        print('# ===== summary =====')
        for k, v in sorted(statistics.items()):
            print(f'{k}{{endpoint="@TOTAL"}}: {v}')
        if statistics['sql_exporter_endpoint_queries_ok'] == 0:
            return 2
        elif statistics['sql_exporter_endpoint_queries_err'] > 0:
            return 1
        else:
            return 0

    def run(self) -> None:
        if self.test_run:
            sys.exit(asyncio.run(self.test()))
        else:
            self.run_app()
# }}}

SqlExporter().run()
