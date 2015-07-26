import asyncio
from collections import deque


DEFAULT_PORT = 11300
DEFAULT_PRIORITY = 2 ** 31
DEFAULT_TTR = 1200
DEFAULT_DELAY = 0


class BeanstalkcException(Exception):
    pass


class UnexpectedResponse(BeanstalkcException):
    pass


class CommandFailed(BeanstalkcException):
    pass


class DeadlineSoon(BeanstalkcException):
    pass


class BeantalkCmd(asyncio.Future):

    def __init__(self, command, expected_ok,
                 expected_err=None,
                 handler=lambda *args: args):
        super().__init__()
        self.command = command
        if not isinstance(command, bytes):
            self.command = command.encode()
        if expected_err is None:
            self.expected_err = []
        else:
            self.expected_err = expected_err
        self.expected_ok = expected_ok
        self.handler = handler


class BeantalkProtocol(asyncio.StreamReaderProtocol):

    def __init__(self):
        super().__init__(
            stream_reader=asyncio.StreamReader(),
            client_connected_cb=self.client_connected,
            loop=loop)
        self._queue = deque()

    @asyncio.coroutine
    def client_connected(self, stream_reader, stream_writer):
        while not stream_reader.at_eof():
            header = yield from stream_reader.readline()
            header = header.decode()
            if header:
                f = self._queue.pop()
                command_name = f.command.split()[0]
                status, *results = header.split()

                if status in f.expected_ok:
                    if asyncio.iscoroutinefunction(f.handler):
                        res = yield from f.handler(*results)
                    else:
                        res = f.handler(*results)
                    f.set_result(res)
                    continue
                elif status in f.expected_err:
                    ex = CommandFailed
                else:
                    ex = UnexpectedResponse
                f.set_exception(ex(command_name, status, results))

    def _interact(self, f):
        self._queue.appendleft(f)
        self._stream_writer.write(f.command)
        return f

    @asyncio.coroutine
    def _handle_job(self, id_, datasize, reserved=True):
        data = yield from self._readexactly(datasize)
        return Job(id_, data, self)

    @asyncio.coroutine
    def _readexactly(self, n):
        n = int(n) + 2
        data = yield from self._stream_reader.readexactly(n)
        return data[:-2]

    def put(self, body, priority=DEFAULT_PRIORITY, delay=0, ttr=DEFAULT_TTR):
        assert isinstance(body, bytes)
        cmd = 'put {} {} {} {}\r\n'.format(priority, delay, ttr, len(body))
        cmd = cmd.encode() + body + b'\r\n'
        f = BeantalkCmd(
            command=cmd,
            expected_ok=['INSERTED'],
            expected_err=['JOB_TOO_BIG', 'BURIED', 'DRAINING'],
            handler=int)
        return self._interact(f)

    def reserve(self, timeout=None):
        if timeout is not None:
            command = 'reserve-with-timeout {}\r\n'.format(timeout)
        else:
            command = 'reserve\r\n'
        f = BeantalkCmd(
            command=command,
            expected_ok=['RESERVED'],
            expected_err=['DEADLINE_SOON', 'TIMED_OUT'],
            handler=self._handle_job)
        return self._interact(f)

    def delete(self, jid):
        f = BeantalkCmd(
            command='delete {}\r\n'.format(jid),
            expected_ok=['DELETED'],
            expected_err=['NOT_FOUND'])
        return self._interact(f)

    def quit(self):
        self._stream_writer.write("quit\r\n".encode())



    def connection_lost(self, exc):
        print('The server closed the connection')
        print('Stop the event lop')
        self._loop.stop()


class Job(object):
    def __init__(self, jid, body, conn, reserved=True):
        self.conn = conn
        self.jid = jid
        self.body = body
        self.reserved = reserved

    @asyncio.coroutine
    def delete(self):
        """Delete this job."""
        yield from self.conn.delete(self.jid)
        self.reserved = False


# loop = asyncio.get_event_loop()
# coro = loop.create_connection(lambda: BeantalkProtocol(), '127.0.0.1', 11300)
#
#
# @asyncio.coroutine
# def debug():
#     _, client = yield from coro
#     for _ in range(100):
#         yield from client.put(b'asd')
#         job = yield from client.reserve()
#         print(job.jid)
#         res = yield from client.peek(int(job.jid))
#         print(res)
#         yield from job.delete()
#     client.quit()
#
# loop.run_until_complete(debug())
# loop.run_forever()
# loop.close()
#
#    def _interact_peek(self, command):
#        try:
#            f = BeantalkCmd(
#                command=command,
#                expected_ok=['FOUND'],
#                expected_err=['NOT_FOUND'],
#                handler=asyncio.coroutine(partial(self._handle_job,
#                                                  reserved=False)))
#            return self._interact(f)
#        except CommandFailed:
#            return None
#
#    def peek(self, jid):
#        """Peek at a job. Returns a Job, or None."""
#        return self._interact_peek('peek {}\r\n'.format(jid))


#    @asyncio.coroutine
#    def _handle_yaml(self, datasize):
#        data = yield from self._readexactly(datasize)
#        return data.decode()
#
#    def tubes(self):
#        f = BeantalkCmd('list-tubes\r\n', ['OK'], handler=self._handle_yaml)
#        return self._interact(f)
#
#    def stats(self):
#        f = BeantalkCmd('stats\r\n', ['OK'], handler=self._handle_yaml)
#        return self._interact(f)
#
#    def stats_tube(self, name):
#        f = BeantalkCmd(
#            command='stats-tube %s\r\n'.format(name),
#            expected_ok=['OK'],
#            expected_err=['NOT_FOUND'],
#            handler=self._handle_yaml)
#        return self._interact(f)
#
#    def watching(self):
#        f = BeantalkCmd('list-tubes-watched\r\n', ['OK'],
#                        handler=self._handle_yaml)
#        return self._interact(f)
#
#    def _interact_peek(self, command):
#        try:
#            f = BeantalkCmd(
#                command=command,
#                expected_ok=['FOUND'],
#                expected_err=['NOT_FOUND'],
#                handler=partial(self._handle_job, reserved=False))
#            return self._interact(f)
#        except CommandFailed:
#            return None
#
#    def peek(self, jid):
#        """Peek at a job. Returns a Job, or None."""
#        return self._interact_peek('peek %d\r\n' % jid)
