import json
import logging
import pkg_resources
import queue
import re
import socket
import ssl
from time import monotonic as now

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Thread, RLock, Event
from typing import Callable, Dict, Match, Optional, Pattern, Tuple, Union
from urllib.parse import urlparse

from pynats.exceptions import (
    NATSConnectionError,
    NATSInvalidResponse,
    NATSInvalidSchemeError,
    NATSRequestTimeoutError,
    NATSTCPConnectionRequiredError,
    NATSTLSConnectionRequiredError,
    NATSUnexpectedResponse,
)
from pynats.nuid import NUID

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
DEFAULT_REQUEST_TIMEOUT = 120.0
DEFAULT_SOCKET_TIMEOUT = 1.0
DEFAULT_WORKERS = 3

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


def log_exception(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            LOG.error(str(e))

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
        "_conn_options",
        "_socket",
        "_socket_buffer",
        "_socket_options",
        "_subs_queue",
        "_ssid",
        "_subs",
        "_nuid",
        "_ping_interval",
        "_waiter",
        "_waiter_enabled",
    )

    def __init__(
        self,
        url: str = "nats://127.0.0.1:4222",
        *,
        name: str = "nats-python",
        verbose: bool = False,
        pedantic: bool = False,
        tls_cacert: Optional[str] = None,
        tls_client_cert: Optional[str] = None,
        tls_client_key: Optional[str] = None,
        tls_verify: bool = False,
        socket_timeout: float = DEFAULT_SOCKET_TIMEOUT,
        socket_keepalive: bool = False,
        ping_interval: float = DEFAULT_PING_INTERVAL,
    ) -> None:
        parsed = urlparse(url)
        self._conn_options = {
            "hostname": parsed.hostname,
            "port": parsed.port,
            "username": parsed.username,
            "password": parsed.password,
            "scheme": parsed.scheme,
            "name": name,
            "lang": "python",
            "protocol": 0,
            "tls_cacert": tls_cacert,
            "tls_client_cert": tls_client_cert,
            "tls_client_key": tls_client_key,
            "tls_verify": tls_verify,
            "version": pkg_resources.get_distribution("nats-python").version,
            "verbose": verbose,
            "pedantic": pedantic,
        }

        self._socket: socket.socket
        self._socket_buffer: bytes = b""
        self._socket_options = {
            "timeout": socket_timeout,
            "keepalive": socket_keepalive,
        }

        self._ssid = 0
        self._nuid = NUID()
        self._ping_interval = ping_interval

    def __enter__(self) -> "NATSNoSubscribeClient":
        self.connect()
        return self

    def __exit__(self, type_, value, traceback) -> None:
        self.close()

    def _send_connect_command(self) -> None:
        options = {
            "name": self._conn_options["name"],
            "lang": self._conn_options["lang"],
            "protocol": self._conn_options["protocol"],
            "version": self._conn_options["version"],
            "verbose": self._conn_options["verbose"],
            "pedantic": self._conn_options["pedantic"],
        }

        if self._conn_options["username"] and self._conn_options["password"]:
            options["user"] = self._conn_options["username"]
            options["pass"] = self._conn_options["password"]
        elif self._conn_options["username"]:
            options["auth_token"] = self._conn_options["username"]

        self._send(CONNECT_OP, json.dumps(options))

    def _connect_tcp(self) -> None:
        self._send_connect_command()
        _command, result = self._recv(INFO_RE)
        if result is None:
            raise NATSConnectionError("connection failed")
        server_info = json.loads(result.group(1))
        if server_info.get("tls_required", False):
            raise NATSTLSConnectionRequiredError("server enabled TLS connection")

    def _connect_tls(self) -> None:
        _command, result = self._recv(INFO_RE)
        if result is None:
            raise NATSConnectionError("connection failed")
        server_info = json.loads(result.group(1))
        if not server_info.get("tls_required", False):
            raise NATSTCPConnectionRequiredError("server disabled TLS connection")

        ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        if self._conn_options["tls_verify"]:
            if self._conn_options["tls_cacert"] is not None:
                ctx.load_verify_locations(cafile=str(self._conn_options["tls_cacert"]))
            if (
                self._conn_options["tls_client_cert"] is not None
                and self._conn_options["tls_client_key"] is not None
            ):
                ctx.load_cert_chain(
                    certfile=str(self._conn_options["tls_client_cert"]),
                    keyfile=str(self._conn_options["tls_client_key"]),
                )
        else:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

        hostname = str(self._conn_options["hostname"])
        self._socket = ctx.wrap_socket(self._socket, server_hostname=hostname)
        self._send_connect_command()
        self._recv(OK_RE)

    def connect(self) -> None:
        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)

        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if self._socket_options["keepalive"]:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        sock.settimeout(self._socket_options["timeout"])
        sock.connect((self._conn_options["hostname"], self._conn_options["port"]))

        self._socket = sock

        scheme = self._conn_options["scheme"]
        if scheme == "nats":
            self._connect_tcp()
        elif scheme == "tls":
            self._connect_tls()
        else:
            raise NATSInvalidSchemeError(f"got unsupported URI scheme: {scheme}")

    def close(self) -> None:
        self._socket.close()
        self._socket_buffer = b""

    def reconnect(self) -> None:
        self.close()
        self.connect()

    def ping(self) -> None:
        self._send(PING_OP)

    def publish(self, subject: str, *, payload: bytes = b"", reply: str = "") -> None:
        self._send(PUB_OP, subject, reply, len(payload))
        self._send(payload)

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

        self._send(SUB_OP, reply_subject, "", 0)
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
        except ssl.SSLError:
            return None, None
        except socket.error as e:
            LOG.error("_read:socket.error:%s", e)
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
            subject=message_data["subject"].decode(),
            reply=message_data["reply"].decode() if message_data["reply"] else "",
            payload=message_payload,
        )
        return message


class NATSClient(NATSNoSubscribeClient):
    __slots__ = (
        "_conn_options",
        "_socket",
        "_socket_buffer",
        "_socket_options",
        "_subs_queue",
        "_send_lock",
        "_ssid",
        "_subs",
        "_nuid",
        "_ping_interval",
        "_waiter",
        "_waiter_enabled",
        "_pinger",
        "_pinger_timer",
        "_workers",
    )

    def __init__(
        self,
        url: str = "nats://127.0.0.1:4222",
        *,
        name: str = "nats-python",
        verbose: bool = False,
        pedantic: bool = False,
        tls_cacert: Optional[str] = None,
        tls_client_cert: Optional[str] = None,
        tls_client_key: Optional[str] = None,
        tls_verify: bool = False,
        socket_timeout: float = DEFAULT_SOCKET_TIMEOUT,
        socket_keepalive: bool = False,
        ping_interval: float = DEFAULT_PING_INTERVAL,
        workers: int = DEFAULT_WORKERS,
    ) -> None:
        parsed = urlparse(url)
        self._conn_options = {
            "hostname": parsed.hostname,
            "port": parsed.port,
            "username": parsed.username,
            "password": parsed.password,
            "scheme": parsed.scheme,
            "name": name,
            "lang": "python",
            "protocol": 0,
            "tls_cacert": tls_cacert,
            "tls_client_cert": tls_client_cert,
            "tls_client_key": tls_client_key,
            "tls_verify": tls_verify,
            "version": pkg_resources.get_distribution("nats-python").version,
            "verbose": verbose,
            "pedantic": pedantic,
            "workers": workers,
        }

        self._socket: socket.socket
        self._socket_buffer: bytes = b""
        self._socket_options = {
            "timeout": socket_timeout,
            "keepalive": socket_keepalive,
        }

        self._ssid = 0
        self._subs: Dict[int, NATSSubscription] = {}
        self._subs_queue: queue.Queue = queue.Queue()
        self._send_lock: RLock = RLock()
        self._nuid = NUID()
        self._waiter: Optional[Thread] = None
        self._waiter_enabled: bool = False
        self._pinger: Optional[Thread] = None
        self._pinger_timer: Event = Event()
        self._ping_interval = ping_interval
        self._workers: Optional[ThreadPoolExecutor] = None

    def connect(self) -> None:
        super().connect()

        self._start_workers()
        self._start_waiter()
        self._start_pinger()

    def close(self) -> None:
        self._stop_pinger()
        self._stop_waiter()
        self._stop_workers()

        super().close()

    def _start_workers(self):
        self._workers = ThreadPoolExecutor(
            max_workers=self._conn_options["workers"], thread_name_prefix="worker"
        )

    def _start_waiter(self):
        self._waiter = Thread(target=self._waiter_thread, args=(self._subs,))
        self._waiter_enabled = True
        self._waiter.start()

    def _start_pinger(self):
        self._pinger_timer.clear()
        self._pinger = Thread(target=self._pinger_thread)
        self._pinger.start()

    def _stop_workers(self):
        if self._workers:
            self._workers.shutdown()
            self._workers = None

    def _stop_waiter(self):
        if self._waiter_enabled:
            self._waiter_enabled = False
        self.ping()
        if self._waiter:
            self._waiter.join()
            self._waiter = None

    def _stop_pinger(self):
        self._pinger_timer.set()
        if self._pinger:
            self._pinger.join()
            self._pinger = None

    @log_exception
    def _pinger_thread(self) -> None:
        while not self._pinger_timer.wait(timeout=self._ping_interval):
            self._send(PING_OP)
        self._pinger_timer.clear()

    def reconnect(self) -> None:
        super().reconnect()
        for sub in self._subs.values():
            self._send(SUB_OP, sub.subject, sub.queue, sub.sid)

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
        self._send(SUB_OP, sub.subject, sub.queue, sub.sid)

        self._stop_waiter()
        self._start_waiter()

        return sub

    def unsubscribe(self, sub: NATSSubscription) -> None:
        self._subs.pop(sub.sid, None)
        self._send(UNSUB_OP, sub.sid)
        self._stop_waiter()
        self._start_waiter()

    def auto_unsubscribe(self, sub: NATSSubscription) -> None:
        if sub.max_messages is None:
            return

        self._send(UNSUB_OP, sub.sid, sub.max_messages)

    def publish(self, subject: str, *, payload: bytes = b"", reply: str = "") -> None:
        with self._send_lock:
            self._send(PUB_OP, subject, reply, len(payload))
            self._send(payload)

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

    @log_exception
    def _waiter_thread(self, subs):
        self._subs = subs
        while self._waiter_enabled:
            command, result = self._recv(MSG_RE, PING_RE, PONG_RE, OK_RE)
            if command is None:
                continue
            if command is MSG_RE:
                self._handle_message(result)
            elif command is PING_RE:
                self._send(PONG_OP)

    def _handle_message(self, result: Match[bytes]) -> None:
        message_data = result.groupdict()

        message_payload_size = int(message_data["size"])
        message_payload = self._readline(size=message_payload_size)
        message_payload = self._strip(message_payload)

        message = NATSMessage(
            sid=int(message_data["sid"].decode()),
            subject=message_data["subject"].decode(),
            reply=message_data["reply"].decode() if message_data["reply"] else "",
            payload=message_payload,
        )

        sub = self._subs[message.sid]
        sub.received_messages += 1

        if sub.is_wasted():
            self._subs.pop(sub.sid, None)

        if self._workers:
            self._workers.submit(sub.callback, message)
