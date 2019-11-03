import json
import logging
import queue
import re
import socket
import ssl
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Event, RLock, Thread
from time import monotonic as now, sleep
from typing import Callable, List, Dict, Match, Optional, Pattern, Tuple, Union
from urllib.parse import urlparse, ParseResult

import pkg_resources

from pynats2.exceptions import (
    NATSConnectionError,
    NATSInvalidResponse,
    NATSInvalidSchemeError,
    NATSInvalidUrlError,
    NATSRequestTimeoutError,
    NATSTCPConnectionRequiredError,
    NATSTLSConnectionRequiredError,
    NATSUnexpectedResponse,
)
from pynats2.nuid import NUID

__all__ = ("NATSSubscription", "NATSMessage", "NATSClient", "NATSNoSubscribeClient")

LOG = logging.getLogger(__name__)

INFO_OP = b"INFO"
CONNECT_OP = b"CONNECT"
PING_OP = b"PING"
PONG_OP = b"PONG"
SUB_OP = b"SUB"
UNSUB_OP = b"UNSUB"
PUB_OP = b"PUB"
MSG_OP = b"MSG"
OK_OP = b"+OK"
ERR_OP = b"-ERR"

DEFAULT_PING_INTERVAL = 30.0
DEFAULT_RECONNECT_DELAY = 10.0
DEFAULT_REQUEST_TIMEOUT = 120.0
DEFAULT_SOCKET_TIMEOUT = 1.0
DEFAULT_WORKERS = 3

RETRY_COUNT = 3
RETRY_INTERVAL = 3

INFO_RE = re.compile(rb"^INFO\s+([^\r\n]+)\r\n")
PING_RE = re.compile(rb"^PING\r\n")
PONG_RE = re.compile(rb"^PONG\r\n")
MSG_RE = re.compile(
    rb"^MSG\s+(?P<subject>[^\s\r\n]+)\s+(?P<sid>[^\s\r\n]+)\s+(?P<reply>([^\s\r\n]+)[^\S\r\n]+)?(?P<size>\d+)\r\n"  # noqa
)
OK_RE = re.compile(rb"^\+OK\s*\r\n")
ERR_RE = re.compile(rb"^-ERR\s+('.+')?\r\n")

_CRLF_ = b"\r\n"
_SPC_ = b" "

COMMANDS = {
    INFO_OP: INFO_RE,
    PING_OP: PING_RE,
    PONG_OP: PONG_RE,
    MSG_OP: MSG_RE,
    OK_OP: OK_RE,
    ERR_OP: ERR_RE,
}

INBOX_PREFIX = bytearray(b"_INBOX.")


def auto_reconnect(func) -> Callable:
    def wrapper(self, *args, **kwargs):
        while True:
            for _ in self._parsed_urls:
                try:
                    return func(self, *args, **kwargs)
                except (socket.error, ssl.SSLError) as e:
                    LOG.error(str(e))
                    self._url_index += 1
                    if self._url_index >= len(self._parsed_urls):
                        self._url_index = 0
                    if func.__name__ != "connect":
                        self.reconnect()

            if not self._reconnect_forever:
                raise NATSConnectionError("all connection failed")

            sleep(self._reconnect_delay)

    return wrapper


def auto_retry(func) -> Callable:
    def wrapper(self, *args, **kwargs):
        while True:
            for _ in range(RETRY_COUNT):
                try:
                    return func(self, *args, **kwargs)
                except (socket.error, ssl.SSLError) as e:
                    LOG.error(str(e))
                    sleep(RETRY_INTERVAL)

            if func.__name__ != "_pinger_thread" and not self._reconnect_forever:
                raise NATSConnectionError("all connection failed")

    return wrapper


@dataclass
class NATSSubscription:
    sid: int
    subject: str
    queue: str
    callback: Callable
    max_messages: Optional[int] = None
    received_messages: int = 0

    def is_wasted(self):
        return (
            self.max_messages is not None
            and self.received_messages == self.max_messages
        )


@dataclass
class NATSMessage:
    sid: int
    subject: str
    reply: str
    payload: bytes


class NATSNoSubscribeClient:
    __slots__ = (
        "_name",
        "_nuid",
        "_parsed_urls",
        "_pedantic",
        "_ping_interval",
        "_reconnect_delay",
        "_reconnect_forever",
        "_socket",
        "_socket_buffer",
        "_socket_keepalive",
        "_socket_timeout",
        "_ssid",
        "_subs",
        "_tls_cacert",
        "_tls_client_cert",
        "_tls_client_key",
        "_tls_verify",
        "_url_index",
        "_verbose",
        "_vhost_name",
        "_vhost_len",
    )

    def __init__(
        self,
        url: str = "nats://127.0.0.1:4222",
        *,
        name: str = "pynats2",
        pedantic: bool = False,
        ping_interval: float = DEFAULT_PING_INTERVAL,
        reconnect: bool = False,
        reconnect_delay: float = DEFAULT_RECONNECT_DELAY,
        reconnect_forever: bool = False,
        tls_cacert: Optional[str] = None,
        tls_client_cert: Optional[str] = None,
        tls_client_key: Optional[str] = None,
        tls_verify: bool = False,
        socket_keepalive: bool = False,
        socket_timeout: float = DEFAULT_SOCKET_TIMEOUT,
        verbose: bool = False,
    ) -> None:
        self._name: str = name
        self._nuid: NUID = NUID()
        self._parsed_urls: List[ParseResult] = []
        self._pedantic: bool = pedantic
        self._ping_interval: float = ping_interval
        self._reconnect_delay: float = reconnect_delay
        self._reconnect_forever = reconnect_forever
        self._socket: socket.socket
        self._socket_buffer: bytes = b""
        self._socket_keepalive: bool = socket_keepalive
        self._socket_timeout: float = socket_timeout
        self._ssid: int = 0
        self._tls_cacert: Optional[str] = tls_cacert
        self._tls_client_cert: Optional[str] = tls_client_cert
        self._tls_client_key: Optional[str] = tls_client_key
        self._tls_verify: bool = tls_verify
        self._url_index: int = 0
        self._verbose: bool = verbose

        for _url in url.split(","):
            try:
                parsed = urlparse(_url)
                self._parsed_urls.append(parsed)
            except ValueError:
                raise NATSInvalidUrlError(_url)

            if parsed.scheme not in ("nats", "tls"):
                raise NATSInvalidSchemeError(
                    f"got unsupported URI scheme: %s" % parsed.scheme
                )

        vhost: str = parsed.path.strip("/").replace("/", ".")
        if len(vhost) > 0:
            vhost += "."
        self._vhost_name: str = vhost
        self._vhost_len: int = len(vhost)

    def __enter__(self) -> "NATSNoSubscribeClient":
        self.connect()
        return self

    def __exit__(self, type_, value, traceback) -> None:
        self.close()

    def _vhost(self, subject: str) -> str:
        if self._vhost_name == "":
            return subject
        return "%s%s" % (self._vhost_name, subject)

    def _del_vhost(self, subject: str) -> str:
        subject = subject.strip()
        if self._vhost_name == "":
            return subject
        if subject.startswith(self._vhost_name):
            return subject[self._vhost_len :]
        return subject

    def _send_connect_command(self) -> None:
        options = {
            "name": self._name,
            "lang": "python",
            "protocol": 0,
            "version": pkg_resources.get_distribution("pynats2").version,
            "verbose": self._verbose,
            "pedantic": self._pedantic,
        }

        username = self._parsed_urls[self._url_index].username
        password = self._parsed_urls[self._url_index].password
        if username and password:
            options["user"] = username
            options["pass"] = password
        elif username:
            options["auth_token"] = username

        self._send(CONNECT_OP, json.dumps(options))

    def _connect_tls(self) -> None:
        ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        if self._tls_verify:
            if self._tls_cacert is not None:
                ctx.load_verify_locations(cafile=self._tls_cacert)
            if self._tls_client_cert is not None and self._tls_client_key is not None:
                ctx.load_cert_chain(
                    certfile=str(self._tls_client_cert),
                    keyfile=str(self._tls_client_key),
                )
        else:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

        hostname = self._parsed_urls[self._url_index].hostname
        self._socket = ctx.wrap_socket(self._socket, server_hostname=hostname)

    @auto_reconnect
    def connect(self) -> None:
        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)

        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if self._socket_keepalive:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        sock.settimeout(self._socket_timeout)
        hostname = self._parsed_urls[self._url_index].hostname
        port = self._parsed_urls[self._url_index].port
        sock.connect((hostname, port))

        self._socket = sock

        _command, result = self._recv(INFO_RE)
        if result is None:
            raise NATSConnectionError("connection failed")

        server_info = json.loads(result.group(1))
        tls = self._parsed_urls[self._url_index].scheme == "tls"
        if server_info.get("tls_required", False) != tls:
            if tls:
                raise NATSTCPConnectionRequiredError("server disabled TLS connection")
            else:
                raise NATSTLSConnectionRequiredError("server enabled TLS connection")

        if tls:
            self._connect_tls()

        self._send_connect_command()

    def close(self) -> None:
        try:
            self._socket.close()
            self._socket_buffer = b""
        except (socket.error, ssl.SSLError):
            pass

    def reconnect(self) -> None:
        self.close()
        self.connect()

    @auto_reconnect
    def ping(self) -> None:
        self._send(PING_OP)

    @auto_reconnect
    def publish(self, subject: str, *, payload: bytes = b"", reply: str = "") -> None:
        self._send(PUB_OP, self._vhost(subject), self._vhost(reply), len(payload))
        self._send(payload)

    @auto_reconnect
    def request(
        self,
        subject: str,
        *,
        payload: bytes = b"",
        timeout: float = DEFAULT_REQUEST_TIMEOUT,
    ) -> Optional[NATSMessage]:
        next_inbox = INBOX_PREFIX[:]
        next_inbox.extend(self._nuid.next_())
        reply_subject = next_inbox.decode()

        self._send(SUB_OP, self._vhost(reply_subject), "", 0)
        self.publish(subject, payload=payload, reply=reply_subject)

        _from_start = now()
        _from_ping = now()
        while True:
            command, result = self._recv(MSG_RE, PING_RE, PONG_RE, OK_RE)
            if command is None:
                if now() - _from_start >= timeout:
                    self._send(UNSUB_OP, 0)
                    raise NATSRequestTimeoutError()
                if now() - _from_ping >= self._ping_interval:
                    _from_ping = now()
                    self.ping()
                continue
            if command is MSG_RE:
                if result is None:
                    # Not reachable
                    return None
                message = self._recv_message(result)
                self._send(UNSUB_OP, 0)
                return message
            elif command is PING_RE:
                self._send(PONG_OP)

    def _send(self, *parts: Union[bytes, str, int]) -> None:
        self._socket.sendall(_SPC_.join(self._encode(p) for p in parts) + _CRLF_)

    def _encode(self, value: Union[bytes, str, int]) -> bytes:
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode()
        elif isinstance(value, int):
            return f"{value:d}".encode()

        raise RuntimeError(f"got unsupported type for encoding: type={type(value)}")

    def _recv(
        self, *commands: Pattern[bytes]
    ) -> Union[Tuple[Pattern[bytes], Match[bytes]], Tuple[None, None]]:
        try:
            line = self._readline()
        except socket.timeout:
            return None, None

        command = self._get_command(line)
        if command not in commands:
            raise NATSUnexpectedResponse(line)

        result = command.match(line)
        if result is None:
            raise NATSInvalidResponse(line)

        return command, result

    def _readline(self, *, size: int = None) -> bytes:
        result: bytes = b""
        if size is None:
            while _CRLF_ not in self._socket_buffer:
                self._socket_buffer += self._socket.recv(4096)
            newline_pos = self._socket_buffer.index(_CRLF_) + len(_CRLF_)
            result = self._socket_buffer[:newline_pos]
            self._socket_buffer = self._socket_buffer[newline_pos:]
        else:
            to_recv_size = size + len(_CRLF_)
            while len(self._socket_buffer) < to_recv_size:
                self._socket_buffer += self._socket.recv(4096)
            result = self._socket_buffer[:to_recv_size]
            self._socket_buffer = self._socket_buffer[to_recv_size:]

        return result

    def _strip(self, line: bytes) -> bytes:
        return line[: -len(_CRLF_)]

    def _get_command(self, line: bytes) -> Optional[Pattern[bytes]]:
        values = self._strip(line).split(b" ", 1)

        return COMMANDS.get(values[0])

    def _recv_message(self, result: Match[bytes]) -> NATSMessage:
        message_data = result.groupdict()

        message_payload_size = int(message_data["size"])
        message_payload = self._readline(size=message_payload_size)
        message_payload = self._strip(message_payload)

        message = NATSMessage(
            sid=int(message_data["sid"].decode()),
            subject=self._del_vhost(message_data["subject"].decode()),
            reply=self._del_vhost(message_data["reply"].decode())
            if message_data["reply"]
            else "",
            payload=message_payload,
        )
        return message


class NATSClient(NATSNoSubscribeClient):
    __slots__ = (
        "_name",
        "_nuid",
        "_parsed_urls",
        "_pedantic",
        "_ping_interval",
        "_pinger",
        "_pinger_timer",
        "_reconnect_delay",
        "_reconnect_forever",
        "_send_lock",
        "_socket",
        "_socket_buffer",
        "_socket_keepalive",
        "_socket_timeout",
        "_ssid",
        "_subs",
        "_tls_cacert",
        "_tls_client_cert",
        "_tls_client_key",
        "_tls_verify",
        "_url_index",
        "_waiter",
        "_waiter_enabled",
        "_workers",
        "_worker_num",
        "_verbose",
        "_vhost_name",
        "_vhost_len",
    )

    def __init__(
        self,
        url: str = "nats://127.0.0.1:4222",
        *,
        name: str = "pynats2",
        pedantic: bool = False,
        ping_interval: float = DEFAULT_PING_INTERVAL,
        reconnect_delay: float = DEFAULT_RECONNECT_DELAY,
        reconnect_forever: bool = False,
        tls_cacert: Optional[str] = None,
        tls_client_cert: Optional[str] = None,
        tls_client_key: Optional[str] = None,
        tls_verify: bool = False,
        socket_keepalive: bool = False,
        socket_timeout: float = DEFAULT_SOCKET_TIMEOUT,
        verbose: bool = False,
        workers: int = DEFAULT_WORKERS,
    ) -> None:
        super().__init__(
            url,
            name=name,
            pedantic=pedantic,
            ping_interval=ping_interval,
            reconnect_delay=reconnect_delay,
            reconnect_forever=reconnect_forever,
            tls_cacert=tls_cacert,
            tls_client_cert=tls_client_cert,
            tls_client_key=tls_client_key,
            tls_verify=tls_verify,
            socket_keepalive=socket_keepalive,
            socket_timeout=socket_timeout,
            verbose=verbose,
        )

        self._pinger: Optional[Thread] = None
        self._pinger_timer: Event = Event()
        self._subs: Dict[int, NATSSubscription] = {}
        self._send_lock: RLock = RLock()
        self._waiter: Optional[Thread] = None
        self._waiter_enabled: bool = False
        self._worker_num: int = workers
        self._workers: Optional[ThreadPoolExecutor] = None

    def _start_workers(self):
        self._workers = ThreadPoolExecutor(
            max_workers=self._worker_num, thread_name_prefix="worker"
        )

    def _stop_workers(self):
        if self._workers:
            self._workers.shutdown()
            self._workers = None

    @auto_reconnect
    def _waiter_thread(self):
        while self._waiter_enabled:
            command, result = self._recv(MSG_RE, PING_RE, PONG_RE, OK_RE)
            if command is None:
                continue
            if command is MSG_RE:
                self._handle_message(result)
            elif command is PING_RE:
                self._send(PONG_OP)

    def _start_waiter(self):
        self._waiter = Thread(target=self._waiter_thread)
        self._waiter_enabled = True
        self._waiter.start()

    def _stop_waiter(self):
        if self._waiter_enabled:
            self._waiter_enabled = False
        try:
            self._send(PING_OP)
        except (socket.error, ssl.SSLError):
            pass
        if self._waiter:
            self._waiter.join()
            self._waiter = None

    @auto_retry
    def _pinger_thread(self) -> None:
        while not self._pinger_timer.wait(timeout=self._ping_interval):
            self._send(PING_OP)
        self._pinger_timer.clear()

    def _start_pinger(self):
        self._pinger_timer.clear()
        self._pinger = Thread(target=self._pinger_thread)
        self._pinger.start()

    def _stop_pinger(self):
        self._pinger_timer.set()
        if self._pinger:
            self._pinger.join()
            self._pinger = None

    def connect(self) -> None:
        super().connect()

        self._start_workers()
        self._start_waiter()
        self._start_pinger()

    def close(self) -> None:
        try:
            self._stop_pinger()
            self._stop_waiter()
            self._stop_workers()

            super().close()
        except (socket.error, ssl.SSLError):
            pass

    def reconnect(self) -> None:
        self.close()
        self.connect()
        for sub in self._subs.values():
            self._send(SUB_OP, self._vhost(sub.subject), sub.queue, sub.sid)

    @auto_retry
    def subscribe(
        self,
        subject: str,
        *,
        callback: Callable,
        queue: str = "",
        max_messages: Optional[int] = None,
    ) -> NATSSubscription:
        sub = NATSSubscription(
            sid=self._ssid,
            subject=subject,
            queue=queue,
            callback=callback,
            max_messages=max_messages,
        )

        self._ssid += 1
        self._subs[sub.sid] = sub
        self._send(SUB_OP, self._vhost(sub.subject), sub.queue, sub.sid)
        return sub

    @auto_retry
    def unsubscribe(self, sub: NATSSubscription) -> None:
        self._subs.pop(sub.sid, None)
        self._send(UNSUB_OP, sub.sid)

    @auto_retry
    def auto_unsubscribe(self, sub: NATSSubscription) -> None:
        if sub.max_messages is None:
            return

        self._send(UNSUB_OP, sub.sid, sub.max_messages)

    @auto_retry
    def publish(self, subject: str, *, payload: bytes = b"", reply: str = "") -> None:
        with self._send_lock:
            self._send(PUB_OP, self._vhost(subject), self._vhost(reply), len(payload))
            self._send(payload)

    @auto_retry
    def request(
        self,
        subject: str,
        *,
        payload: bytes = b"",
        timeout: float = DEFAULT_REQUEST_TIMEOUT,
    ) -> NATSMessage:
        next_inbox = INBOX_PREFIX[:]
        next_inbox.extend(self._nuid.next_())
        reply_subject = next_inbox.decode()
        reply_queue: queue.Queue = queue.Queue()

        def callback(message: NATSMessage) -> None:
            reply_queue.put(message)

        sub = self.subscribe(reply_subject, callback=callback, max_messages=1)
        self.publish(subject, payload=payload, reply=reply_subject)

        try:
            return reply_queue.get(timeout=timeout)
        except queue.Empty:
            raise NATSRequestTimeoutError()
        finally:
            self.unsubscribe(sub)

    def _send(self, *parts: Union[bytes, str, int]) -> None:
        with self._send_lock:
            self._socket.sendall(_SPC_.join(self._encode(p) for p in parts) + _CRLF_)

    def _handle_message(self, result: Match[bytes]) -> None:
        message_data = result.groupdict()

        message_payload_size = int(message_data["size"])
        message_payload = self._readline(size=message_payload_size)
        message_payload = self._strip(message_payload)

        message = NATSMessage(
            sid=int(message_data["sid"].decode()),
            subject=self._del_vhost(message_data["subject"].decode()),
            reply=self._del_vhost(message_data["reply"].decode())
            if message_data["reply"]
            else "",
            payload=message_payload,
        )

        sub = self._subs.get(message.sid)
        if sub is None:
            LOG.error("no subscribe")
            return
        sub.received_messages += 1

        if sub.is_wasted():
            self._subs.pop(sub.sid, None)

        if self._workers:
            self._workers.submit(sub.callback, message)
