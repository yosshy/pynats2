import os
import socket
import threading
import time

import msgpack
import pytest

from pynats2 import (
    NATSClient,
    NATSInvalidSchemeError,
    NATSNoSubscribeClient,
    NATSRequestTimeoutError,
)

event = threading.Event()


@pytest.fixture
def nats_url():
    return os.environ.get("NATS_URL", "nats://127.0.0.1:4222")


def test_connect_and_close(nats_url):
    client = NATSClient(nats_url)

    client.connect()
    client.ping()
    client.close()


def test_connect_and_close_using_context_manager(nats_url):
    with NATSClient(nats_url) as client:
        client.ping()


def test_connect_timeout():
    client = NATSClient("nats://127.0.0.1:4223")

    with pytest.raises(socket.error):
        client.connect()


def test_connect_multiple_urls(nats_url):
    urls = nats_url + "," + nats_url
    with NATSClient(urls) as client:
        client.ping()


def test_reconnect(nats_url):
    client = NATSClient(nats_url)

    client.connect()
    client.ping()

    client.reconnect()
    client.ping()

    client.close()


def test_tls_connect():
    client = NATSClient("tls://127.0.0.1:4224", verbose=True)

    client.connect()
    client.ping()
    client.close()


def test_invalid_scheme():
    with pytest.raises(NATSInvalidSchemeError):
        NATSClient("http://127.0.0.1:4224", verbose=True)


def test_subscribe_unsubscribe(nats_url):
    with NATSClient(nats_url) as client:
        sub = client.subscribe(
            "test-subject", callback=lambda x: x, queue="test-queue", max_messages=2
        )
        client.unsubscribe(sub)


def test_publish(nats_url):
    received = []

    def worker():
        with NATSClient(nats_url) as client:

            def callback(message):
                received.append(message)

            client.subscribe(
                "test-subject", callback=callback, queue="test-queue", max_messages=2
            )
            event.wait()

    t = threading.Thread(target=worker)
    t.start()

    time.sleep(0.1)

    with NATSClient(nats_url) as client:
        # publish without payload
        client.publish("test-subject")
        # publish with payload
        client.publish("test-subject", payload=b"test-payload")

    event.set()
    event.clear()
    t.join()

    assert len(received) == 2

    assert received[0].subject == "test-subject"
    assert received[0].reply == ""
    assert received[0].payload == b""

    assert received[1].subject == "test-subject"
    assert received[1].reply == ""
    assert received[1].payload == b"test-payload"


def test_send_only_publish(nats_url):
    received = []

    def worker():
        with NATSClient(nats_url) as client:

            def callback(message):
                received.append(message)

            client.subscribe(
                "test-subject", callback=callback, queue="test-queue", max_messages=2
            )
            event.wait()

    t = threading.Thread(target=worker)
    t.start()

    time.sleep(0.1)

    with NATSNoSubscribeClient(nats_url) as client:
        # publish without payload
        client.publish("test-subject")
        # publish with payload
        client.publish("test-subject", payload=b"test-payload")

    time.sleep(0.1)
    event.set()
    event.clear()
    t.join()

    assert len(received) == 2

    assert received[0].subject == "test-subject"
    assert received[0].reply == ""
    assert received[0].payload == b""

    assert received[1].subject == "test-subject"
    assert received[1].reply == ""
    assert received[1].payload == b"test-payload"


def test_request(nats_url):
    def worker():
        with NATSClient(nats_url) as client:

            def callback(message):
                client.publish(message.reply, payload=b"test-callback-payload")

            client.subscribe(
                "test-subject", callback=callback, queue="test-queue", max_messages=2
            )
            event.wait()

    t = threading.Thread(target=worker)
    t.start()

    time.sleep(0.1)

    with NATSClient(nats_url) as client:
        # request without payload
        resp = client.request("test-subject")
        assert resp.subject.startswith("_INBOX.")
        assert resp.reply == ""
        assert resp.payload == b"test-callback-payload"

        # request with payload
        resp = client.request("test-subject", payload=b"test-payload")
        assert resp.subject.startswith("_INBOX.")
        assert resp.reply == ""
        assert resp.payload == b"test-callback-payload"

    event.set()
    event.clear()
    t.join()


def test_send_only_request(nats_url):
    def worker():
        with NATSClient(nats_url) as client:

            def callback(message):
                client.publish(message.reply, payload=b"test-callback-payload")

            client.subscribe(
                "test-subject", callback=callback, queue="test-queue", max_messages=2
            )
            event.wait()

    t = threading.Thread(target=worker)
    t.start()

    time.sleep(0.1)

    with NATSNoSubscribeClient(nats_url) as client:
        # request without payload
        resp = client.request("test-subject")
        assert resp.subject.startswith("_INBOX.")
        assert resp.reply == ""
        assert resp.payload == b"test-callback-payload"

        # request with payload
        resp = client.request("test-subject", payload=b"test-payload")
        assert resp.subject.startswith("_INBOX.")
        assert resp.reply == ""
        assert resp.payload == b"test-callback-payload"

    event.set()
    event.clear()
    t.join()


def test_request_msgpack(nats_url):
    def worker():
        with NATSClient(nats_url) as client:

            def callback(message):
                client.publish(
                    message.reply,
                    payload=msgpack.packb(
                        {b"v": 3338} if message.payload else {b"v": 32}
                    ),
                )

            client.subscribe(
                "test-subject", callback=callback, queue="test-queue", max_messages=2
            )
            event.wait()

    t = threading.Thread(target=worker)
    t.start()

    time.sleep(0.1)

    with NATSClient(nats_url) as client:
        # request without payload
        resp = client.request("test-subject")
        assert resp.subject.startswith("_INBOX.")
        assert resp.reply == ""
        assert msgpack.unpackb(resp.payload) == {b"v": 32}

        # request with payload
        resp = client.request("test-subject", payload=msgpack.packb("test-payload"))
        assert resp.subject.startswith("_INBOX.")
        assert resp.reply == ""
        assert msgpack.unpackb(resp.payload) == {b"v": 3338}

    event.set()
    event.clear()
    t.join()


def test_request_timeout(nats_url):
    with NATSClient(nats_url) as client:
        with pytest.raises(NATSRequestTimeoutError):
            client.request("test-subject", timeout=2)
