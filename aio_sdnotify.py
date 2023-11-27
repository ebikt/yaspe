# python3
import asyncio, socket
import os, stat, sys
from typing import Optional, List, Union, Callable, Tuple, IO

class Connection:
    async def send(self, buf:bytes) -> None:
        pass
    async def close(self) -> None:
        pass

class DgramUnixConnection(Connection):
    proto: asyncio.DatagramProtocol
    trans: asyncio.DatagramTransport

    async def connect(self, path:Optional[str] = None, sock:Optional[socket.socket] = None) -> None:
        self.proto = asyncio.DatagramProtocol()
        self.trans, _ = await asyncio.get_event_loop().create_datagram_endpoint( # type: ignore
            lambda: self.proto,
            remote_addr = path,
            sock = sock,
            family = socket.AF_UNIX if sock is None else 0
        )

    async def send(self, buf:bytes) -> None:
        self.trans.sendto(buf) #no async, Just write and forget.

    async def close(self) -> None:
        self.trans.close()

class StreamUnixConnection(Connection):

    async def connect(self, path:Optional[str] = None, sock:Optional[socket.socket] = None) -> None:
        self.reader, self.writer = await asyncio.open_unix_connection(
            path = path, sock = sock
        )

    async def send(self, buf:bytes) -> None:
        self.writer.write(buf) #no async, Just write and forget.

    async def close(self) -> None:
        self.writer.close()

class BlockingFileConnection(Connection):
    async def connect(self, fd:IO[bytes]) -> None:
        sys.stderr.write('sdnotify Warning: using blocking file connection\n')
        sys.stderr.flush()
        self.fd = fd

    async def send(self, buf:bytes) -> None:
        self.fd.write(buf)
        self.fd.flush()

    async def close(self) -> None:
        self.fd.close()

class SystemdNotifier:
    conn: Optional[Connection] = None
    debug = False

    def __init__(self, path:Optional[str] = None, unset:bool = True) -> None:
        if path is None:
            self.path = os.getenv('NOTIFY_SOCKET')
        else:
            self.path = path
        if unset:
            os.unsetenv('NOTIFY_SOCKET')

    async def connect(self, path:Optional[str] = None) -> Connection:
        if path is None:
            path = self.path
        if path is None:
            raise Exception("Not running in systemd? no path passed to SystemdNotifier.connect()")
        if path[:1]=='@':
            path = '\0' + path[1:]
        elif path.isdecimal():
            try:
                sock = socket.socket(int(path))
            except Exception:
                pass
            else: # No exception
                if sock.type == socket.SOCK_DGRAM:
                    duc = DgramUnixConnection()
                    await duc.connect(sock = sock)
                    return duc
                if sock.type == socket.SOCK_STREAM:
                    suc = StreamUnixConnection()
                    await suc.connect(sock = sock)
                    return suc
            bfc = BlockingFileConnection()
            await bfc.connect(os.fdopen(int(path), 'wb'))
            return bfc
        elif not stat.S_ISSOCK(os.stat(path).st_mode):
            bfc = BlockingFileConnection()
            await bfc.connect(open(path, 'ab'))
            return bfc
        duc = DgramUnixConnection()
        await duc.connect(path = path)
        return duc

    async def notify(self, state:Union[None, bytes, str] = None, catch:bool = True, **statekw:Union[bytes,str,int]) -> bool:
        send:List[bytes] = []
        for k, v in statekw.items():
            send.append(k.upper().encode('ascii'))
            send.append(b'=')
            if not isinstance(v, bytes):
                v = str(v).encode('utf-8')
            v = v.rstrip(b'\n').replace(b'\n', b'  ')
            send.append(v)
            send.append(b'\n')
        if state:
            if not isinstance(state, bytes):
                state = str(state).encode('utf-8')
            if len(statekw) or b'=' not in state:
                send.append(b'STATUS=')
            if b'\n' in state[:-1]:
                state = state.rstrip(b'\n').replace(b'\n',b'  ')
            send.append(state)
            if state[-1:] != b'\n':
                send.append(b'\n')

        try:
            c = self.conn
            if c is None:
                c = await self.connect()
                self.conn = c
            await c.send(b''.join(send))
        except Exception as e:
            if self.conn:
                try:
                    await self.conn.close()
                except Exception:
                    pass
                finally:
                    self.conn = None
            if not catch:
                raise
            return False
        return True

if __name__ == '__main__':
    import time

    class Test:
        async def test_notify(self, s:SystemdNotifier)->None:
            await s.notify('s:test', catch=False)
            await s.notify(b'b:test', catch=False)
            await asyncio.sleep(.5)
            await s.notify(ready=1,status='s:test',xfoo=b'b:bar', catch=False)
            await asyncio.sleep(.5)

        def run(self) -> None:
            child_pid = -1
            try:
                self.setup()
                sd = SystemdNotifier()
                sd.debug = True
                child_pid = os.fork()
                if child_pid == 0:
                    self.cleanup_fork()
                    asyncio.run(self.test_notify(sd))
                    sys.exit(0)
                loop = True
                while loop:
                    rv = os.waitid(os.P_PID, child_pid, os.WEXITED | os.WNOHANG)
                    if rv is not None:
                        assert rv.si_pid == child_pid
                        if rv.si_code != os.CLD_EXITED:
                            raise Exception(f"Abnormal subprocess termination {rv}")
                        if rv.si_status != 0:
                            raise Exception(f"Subprocess terminated with nonzero exit code {rv.si_status}")
                        loop = False
                    try:
                        data = self.recv()
                        if data is not None:
                            print(f'Received: {data!r}')
                    except BlockingIOError:
                        pass
                    time.sleep(0.1)
            finally:
                if child_pid != 0:
                    self.cleanup()

        def setup(self) -> None: pass
        def cleanup_fork(self) -> None: pass
        def recv(self) -> Optional[bytes]: return None
        def cleanup(self) -> None: pass

    class Test_sock(Test):
        def setup(self) -> None:
            sockname = f'TEST-SOCKET-{os.getpid()}-{time.time()}'
            os.environ['NOTIFY_SOCKET']='@' + sockname
            sockname = '\x00' + sockname
            l = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            l.bind(sockname)
            l.setblocking(False)
            self.sock = l

        def cleanup_fork(self) -> None:
            self.sock.close()

        def recv(self) -> Optional[bytes]:
            return self.sock.recv(65536)

        def cleanup(self) -> None:
            self.sock.close()

    class Test_fd(Test):
        r:int
        w:int
        def setup(self) -> None:
            self.r, self.w = os.pipe()
            os.environ['NOTIFY_SOCKET']=f'{self.w}'
            import fcntl
            fcntl.fcntl(self.r, fcntl.F_SETFL, fcntl.fcntl(self.r, fcntl.F_GETFL) | os.O_NONBLOCK)

        def cleanup_fork(self) -> None:
            os.close(self.r)

        def recv(self) -> Optional[bytes]:
            return os.read(self.r, 65536)

        def cleanup(self) -> None:
            os.close(self.r)
            os.close(self.w)

    class Test_pipe(Test):
        def setup(self) -> None:
            self.pipename = f'/tmp/test-pipe-{os.getpid()}-{time.time()}'
            os.mkfifo(self.pipename)
            os.environ['NOTIFY_SOCKET']=self.pipename

        def recv(self) -> Optional[bytes]:
            if not hasattr(self, 'r'):
                self.r = open(self.pipename, 'rb')
                import fcntl
                fcntl.fcntl(self.r, fcntl.F_SETFL, fcntl.fcntl(self.r, fcntl.F_GETFL) | os.O_NONBLOCK)
            b = self.r.read()
            return None if b == b'' else b

        def cleanup(self) -> None:
            os.unlink(self.pipename)
            self.r.close()

    if len(sys.argv) > 2:
        arg = 'invalid'
    elif len(sys.argv) == 2:
        arg = sys.argv[1]
    else:
        arg = 'sock'

    from typing import cast, Dict, Type
    t = cast(Dict[str,Type[Test]], globals()).get('Test_' + arg, None)
    if isinstance(t, cast(Type[Test],type)) and issubclass(t, Test):
        t().run()
    else:
        print("Usage: python3 aio_sdnotify.py [sock|fd|pipe]")
