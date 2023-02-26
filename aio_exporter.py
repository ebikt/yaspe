#!/usr/bin/python3
#
# Asynchronous exporter with caching and request demultiplexing
#

import aiohttp
import asyncio
from   aiohttp import web
import socket
import inspect
import sys
import re
import expiringdict # type: ignore

from aio_sdnotify import SystemdNotifier

from typing import List, Union, Iterator, Dict, Callable, Awaitable, Optional, Tuple, cast, Type, Iterable, TypeVar


def labelEncode(val:Union[int,str,bytes]) -> str:
    if isinstance(val, bytes):
        return val.decode('utf-8')
    if not isinstance(val, str):
        return str(val)
    return val

class MetricValue:
    def __init__(self, value:Union[int, float], labels:List[str], additional_labels:Dict[str,str]) -> None:
        self.value  = value
        self.labels = labels
        self.additional_labels = additional_labels

class InfluxRow:
    def __init__(self, labels:List[str], str_values:Dict[str,str], num_values:Dict[str,Union[int,float]]) -> None:
        self.labels = labels
        self.str_values = str_values
        self.num_values = num_values

def is_valid_label(s:str) -> bool:
    return s.isascii() and (s.isidentifier() or s.replace(':','').isidentifier())

T = TypeVar('T')
class BaseMetric:
    def __init__(self, id:Union[str, bytes], labels:Tuple[Union[str, bytes], ...]) -> None:
        if isinstance(id, bytes):
            id = id.decode('ascii')
        assert is_valid_label(id)
        self.id = id

        self.values:Dict[Tuple[str, ...], MetricValue] = {}
        self.labels = [ labelEncode(k) for k in labels ]
        self.labels_i = { l:i for i, l in enumerate(self.labels) }
        for l in self.labels: assert is_valid_label(l)

    def collect_labels(self,
            labels_a:Tuple[Union[int,str,bytes], ...],
            labels_kw:Dict[str, T],
        ) -> Tuple[List[str], Dict[str, T]]:
        assert len(labels_a) <= len(self.labels)
        labels =  [ labelEncode(l) for l in labels_a ]
        unseen = set(range(len(labels_a),len(self.labels)))
        labels = labels + [''] * len(unseen)
        remains_kw:Dict[str, T] = {}
        for k, v in labels_kw.items():
            try:
                i = self.labels_i[labelEncode(k)]
            except KeyError:
                remains_kw[k] = v
            else:
                unseen.remove(i)
                if isinstance(v, (int, str, bytes)):
                    labels[i] = labelEncode(v)
                else:
                    raise ValueError("Float dimension is not allowed")
        assert len(unseen) == 0
        return labels, remains_kw

    def render(self, format:str) -> Iterator[str]:
        raise NotImplementedError('virtual')

    INFLUX_SLUGIFY=re.compile('(?:\s|[,"\'=])')


class Metric(BaseMetric):
    def __init__(self, id:Union[str, bytes], type:Union[str,bytes], help:Union[str,bytes], *labels:Union[str, bytes]) -> None:
        super().__init__(id = id, labels = labels)
        if isinstance(type, bytes):
            type = type.decode('ascii')
        if isinstance(help, bytes):
            help = help.decode('utf-8')
        assert help.isprintable()
        type = type.lower()
        help = help.replace('\\', '\\\\').replace('\n', '\\\n')
        assert type in ('counter', 'gauge', 'untyped')
        self.header = f"# HELP {self.id} {help}\n# TYPE {self.id} {type}\n"
        self.type = type

        self.values:Dict[Tuple[str, ...], MetricValue] = {}

    def collect(self, value:Union[int, float], *labels_a:Union[int,str,bytes],
            additional_labels:Union[None, Dict[str, str], Dict[str, bytes], Dict[bytes, bytes]] = None,
            **labels_kw:Union[int,str,bytes],
        ) -> None:
        labels, remains_kw = self.collect_labels( labels_a = labels_a, labels_kw = labels_kw)
        if len(remains_kw):
            raise ValueError(f"Uknown labels {remains_kw!r}")

        al = {}
        if additional_labels is not None:
            for kb, vb in additional_labels.items():
                k = labelEncode(kb)
                assert k not in self.labels
                al[k] = labelEncode(vb)

        self.values[tuple(labels)] = MetricValue(value, labels, al)

    def format_value(self, value:MetricValue) -> str:
        if isinstance(value.value, int): # True and False are instances of int
            return f"{value.value:d}"
        elif isinstance(value.value, float):
            return f"{value.value:.15e}"
        else:
            assert False

    def render_prometheus_value(self, value:MetricValue) -> Iterator[str]:
        yield self.id
        first = True
        for i, k in enumerate(self.labels):
            if first:
                yield '{'
                first = False
            else:
                yield '",'
            yield k
            yield '="'
            yield value.labels[i].replace('\\','\\\\').replace('\n', '\\\n').replace('"', '\\"')
        for k, v in value.additional_labels.items():
            if first:
                yield '{'
                first = False
            else:
                yield '",'
            yield k
            yield '="'
            yield v.replace('\\','\\\\').replace('\n', '\\\n').replace('"', '\\"')
        if first:
            yield ' '
        else:
            yield '"} '
        yield self.format_value(value)

    def render_influx_value(self, value:MetricValue) -> Iterator[str]:
        yield self.id
        for i, k in enumerate(self.labels):
            yield ','
            yield k
            yield '='
            yield self.INFLUX_SLUGIFY.sub('_', value.labels[i])
        yield ' '
        for v, k in value.additional_labels.items():
            yield k
            yield '='
            yield self.INFLUX_SLUGIFY.sub('_', v)
            yield ','
        yield self.type
        yield '='
        yield self.format_value(value)

    def render(self, format:str) -> Iterator[str]:
        if format == 'prometheus':
            yield self.header
            for v in self.values.values():
                yield from self.render_prometheus_value(v)
                yield '\n'
        elif format == 'influx':
            for v in self.values.values():
                yield from self.render_influx_value(v)
                yield '\n'

class InfluxMetric(BaseMetric):
    def __init__(self, id:Union[str, bytes], *labels:Union[str, bytes]) -> None:
        super().__init__(id = id, labels = labels)
        self.rows:Dict[Tuple[str, ...], InfluxRow] = {}

    def collect(self,
            *labels_a:Union[int,str,bytes],
            **values:Union[int, float, str, bytes],
        ) -> None:
        labels, remains_kw = self.collect_labels( labels_a = labels_a, labels_kw = values)

        str_values: Dict[str, str] = {}
        num_values: Dict[str, Union[int, float]] = {}
        for k, v in remains_kw.items():
            if isinstance(v, bytes):
                str_values[k] = v.decode('utf-8')
            elif isinstance(v, str):
                str_values[k] = v
            elif isinstance(v, (float, int)):
                num_values[k] = v
            else:
                raise ValueError(f"Invalid value of {k!r}: {v!r}")
        self.rows[tuple(labels)] = InfluxRow(labels, str_values, num_values)

    def format_value(self, value:Union[int, float]) -> str:
        if isinstance(value, int): # True and False are instances of int
            return f"{value:d}"
        elif isinstance(value, float):
            return f"{value:.15e}"
        else:
            assert False

    def render_influx_row(self, row:InfluxRow) -> Iterator[str]:
        yield self.id
        for i, k in enumerate(self.labels):
            yield ','
            yield k
            yield '='
            yield self.INFLUX_SLUGIFY.sub('_', row.labels[i])
        sep = ' '
        for k, vs in row.str_values.items():
            yield sep
            sep = ','
            yield k
            yield '='
            yield self.INFLUX_SLUGIFY.sub('_', vs)
        for k, vn in row.num_values.items():
            yield sep
            sep = ','
            yield k
            yield '='
            yield self.format_value(vn)

    def render(self, format:str) -> Iterator[str]:
        assert format == 'influx'
        for v in self.rows.values():
            yield from self.render_influx_row(v)
            yield '\n'

class Exporter:
    host:Optional[Union[str,web.HostSequence]] = None
    port:Optional[int] = None
    path:Optional[str] = None
    sock:Optional[socket.socket] = None
    reuse_address = True
    reuse_port    = True
    metrics_cache_sec  = 10 # How long (from _start_ of request) to keep result cached
    metrics_cache_size = 1000
    collect_timeout    = 10 # Wait at most this time for collect. Then abort collection (and return timeout to all waiters).
    allowed_formats:Tuple[str, ...] = ('prometheus', 'influx')

    name='TEST Exporter'

    def get_pages(self) -> Iterator[Tuple[str, Callable[[aiohttp.web.Request], Awaitable[web.Response]], str]]:
        for k in dir(self):
            if k.startswith('webpage_'):
                v = cast(Callable[[aiohttp.web.Request], Awaitable[web.Response]], getattr(self, k))
                v2 = v #mypy 0.991 bug/feature? `Any` sneaks into type of v2 after next line, thus isolating in different variable
                if not inspect.ismethod(v2) and not inspect.iscoroutinefunction(v2): continue
                help = inspect.getdoc(v)
                assert help is not None
                yield k[7:].replace('_','/'), v, help

    async def webpage_(self, request:aiohttp.web.Request) -> web.Response:
        """ This page """
        resp = [f'<html>\n<head><title>{self.name}</title></head>\n<body><h1>{self.name}</h1><p>\n']
        for path, handler, help in self.get_pages():
            resp.append(f'<a href="{path}">{path}</a> - {help}<br>\n')
        resp.append('</body></html>\n')
        return web.Response(text=''.join(resp), content_type='text/html')

    def get_format(self, request:aiohttp.web.Request) -> str:
        for v in request.query.getall('format', []):
            if v in self.allowed_formats:
                return v
        return self.allowed_formats[0]

    async def webpage_metrics(self, request:aiohttp.web.Request) -> web.Response:
        """ Metrics """
        m = await self.collect_or_cached_metrics(request)

        return web.Response(text=self.render(m, format=self.get_format(request)))

    metrics_cache: Dict[str, Awaitable[List[BaseMetric]]]

    def sdn(self) -> SystemdNotifier:
        if not hasattr(self, '_sdn'):
            self._sdn = SystemdNotifier()
        return self._sdn

    sd_start_msg = 'Starting'
    async def notify_start(self, msg:Optional[str] = None) -> None:
        if self.sd_start_msg or msg:
            if msg is None:
                msg = self.sd_start_msg
            if msg:
                await self.sdn().notify(status=msg)
            self.sd_start_msg = ''

    sd_ready_msg = 'Ready'
    async def notify_ready(self, msg:Optional[str] = None) -> None:
        if self.sd_ready_msg or msg:
            if msg is None:
                msg = self.sd_ready_msg
            if msg:
                await self.sdn().notify(status=msg, ready=1)
            else:
                await self.sdn().notify(ready=1)
            self.sd_ready_msg = ''

    def __init__(self) -> None:
        self.app = web.Application()
        self.app.add_routes([web.get(path, handler) for path, handler, _ in self.get_pages()])
        self.metrics_cache = expiringdict.ExpiringDict(max_len=self.metrics_cache_size, max_age_seconds = self.metrics_cache_sec) # type: ignore

    def sync_config(self) -> None:
        pass

    sdnotify_tasks:List[Awaitable[None]] = []

    async def notify_and_return_app(self) -> web.Application:
        await self.notify_start()
        return self.app

    def run_app(self) -> None:
        self.sync_config()
        self.app.on_cleanup.append(self.on_cleanup_signal)
        self.app.on_shutdown.append(self.on_shutdown_signal)
        web.run_app(self.notify_and_return_app(),
            host=self.host, port=self.port, path=self.path, sock=self.sock,
            reuse_address = self.reuse_address, reuse_port = self.reuse_port,
            print = self.on_ready,
        )

    def on_ready(self, msg:str) -> None:
        if sys.stdout.isatty():
            print(msg)
        msg = msg.replace('\n(Press CTRL+C to quit)','')
        self.sdnotify_tasks.append(asyncio.create_task(self.on_ready_async(msg)))

    async def on_ready_async(self, msg:str) -> None:
        await self.notify_ready(msg)

    async def on_cleanup_signal(self, _:web.Application) -> None:
        for task in self.sdnotify_tasks:
            await task

    async def on_shutdown_signal(self, _:web.Application) -> None:
        await self.sdn().notify(status="Shutdown")

    def render_metrics(self, metrics:List[BaseMetric], format:str) -> Iterator[str]:
        for metric in metrics:
            yield from metric.render(format)

    def render(self, metrics:List[BaseMetric], format:str = 'prometheus') -> str:
        return ''.join(self.render_metrics(metrics, format=format))

    async def collect_or_cached_metrics(self, request:aiohttp.web.Request) -> List[BaseMetric]:
        hash = self.get_req_hash(request)
        try:
            task = self.metrics_cache[hash]
        except KeyError:
            task = asyncio.create_task(self.collect_metrics(request))
            self.metrics_cache[hash] = task
        try:
            return await asyncio.wait_for(task, timeout = self.collect_timeout)
        except TimeoutError:
            if self.metrics_cache[hash] == task:
                del self.metrics_cache[hash]
            raise

    def get_req_hash(self, request:aiohttp.web.Request) -> str:
        return request.path_qs

    async def collect_metrics(self, request:aiohttp.web.Request) -> List[BaseMetric]:
        m = Metric('test', 'gauge', 'Default metric for testing purposes', 'label')
        import time
        m.collect(time.time(),'just a test')
        return [m]

if __name__ == '__main__':
    class TestExporter(Exporter):
        async def collect_metrics(self, request:aiohttp.web.Request) -> List[BaseMetric]:
            if self.get_format(request) == 'influx':
                m = InfluxMetric('test', 'label1', 'label2')
                m.collect('x', label2='y', txtvalue='z', numvalue=1, fvalue=2.3)
                return [m]
            else:
                return await super().collect_metrics(request)
    TestExporter().run_app()
