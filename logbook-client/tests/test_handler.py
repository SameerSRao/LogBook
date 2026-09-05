import json
import logging
import time

import httpx
import pytest
import respx

from logbook_client import LogBookHandler

INGEST_URL = "http://logbook-ingest"


@respx.mock
def test_handler_sends_log_to_ingest_api():
    route = respx.post(f"{INGEST_URL}/logs").mock(
        return_value=httpx.Response(200, json={"accepted": 1})
    )

    handler = LogBookHandler(ingest_url=INGEST_URL, service="test-app", flush_interval=0.1)
    logger = logging.getLogger("test_sends_log")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    logger.info("hello from test", extra={"context": {"user_id": 99}})
    time.sleep(0.3)
    handler.close()

    assert route.called
    body = json.loads(route.calls[0].request.content)
    log = body["logs"][0]
    assert log["service"] == "test-app"
    assert log["level"] == "INFO"
    assert log["message"] == "hello from test"
    assert log["context"] == {"user_id": 99}


@respx.mock
def test_handler_maps_warning_to_warn():
    route = respx.post(f"{INGEST_URL}/logs").mock(
        return_value=httpx.Response(200, json={"accepted": 1})
    )

    handler = LogBookHandler(ingest_url=INGEST_URL, service="svc", flush_interval=0.1)
    logger = logging.getLogger("test_warn_level")
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)

    logger.warning("a warning")
    time.sleep(0.3)
    handler.close()

    body = json.loads(route.calls[0].request.content)
    assert body["logs"][0]["level"] == "WARN"


@respx.mock
def test_handler_maps_critical_to_error():
    route = respx.post(f"{INGEST_URL}/logs").mock(
        return_value=httpx.Response(200, json={"accepted": 1})
    )

    handler = LogBookHandler(ingest_url=INGEST_URL, service="svc", flush_interval=0.1)
    logger = logging.getLogger("test_critical_level")
    logger.addHandler(handler)
    logger.setLevel(logging.CRITICAL)

    logger.critical("system down")
    time.sleep(0.3)
    handler.close()

    body = json.loads(route.calls[0].request.content)
    assert body["logs"][0]["level"] == "ERROR"


@respx.mock
def test_handler_never_raises_on_failed_send():
    respx.post(f"{INGEST_URL}/logs").mock(side_effect=httpx.ConnectError("refused"))

    handler = LogBookHandler(ingest_url=INGEST_URL, service="svc", flush_interval=0.1)
    logger = logging.getLogger("test_no_raise")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    logger.info("this should not raise")
    time.sleep(0.3)
    handler.close()
    # reaching here without exception = pass


@respx.mock
def test_handler_batches_multiple_logs():
    route = respx.post(f"{INGEST_URL}/logs").mock(
        return_value=httpx.Response(200, json={"accepted": 3})
    )

    handler = LogBookHandler(ingest_url=INGEST_URL, service="batch-svc", flush_interval=0.1)
    logger = logging.getLogger("test_batch")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    logger.debug("one")
    logger.info("two")
    logger.error("three")
    time.sleep(0.3)
    handler.close()

    body = json.loads(route.calls[0].request.content)
    assert len(body["logs"]) == 3
    assert body["logs"][0]["message"] == "one"
    assert body["logs"][2]["message"] == "three"
