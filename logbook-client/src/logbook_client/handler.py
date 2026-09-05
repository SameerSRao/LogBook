import logging
import queue
import threading
from datetime import datetime, timezone

import httpx

LEVEL_MAP = {
    logging.DEBUG: "DEBUG",
    logging.INFO: "INFO",
    logging.WARNING: "WARN",
    logging.ERROR: "ERROR",
    logging.CRITICAL: "ERROR",
}


class LogBookHandler(logging.Handler):
    def __init__(
        self,
        ingest_url: str,
        service: str,
        batch_size: int = 50,
        flush_interval: float = 5.0,
    ):
        super().__init__()
        self._ingest_url = ingest_url.rstrip("/") + "/logs"
        self._service = service
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._queue: queue.Queue = queue.Queue()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._thread.start()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            entry = {
                "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
                "service": self._service,
                "level": LEVEL_MAP.get(record.levelno, "INFO"),
                "message": self.format(record),
                "context": getattr(record, "context", None),
            }
            self._queue.put_nowait(entry)
            if self._queue.qsize() >= self._batch_size:
                self._flush()
        except Exception:
            self.handleError(record)

    def _flush(self) -> None:
        batch = []
        try:
            while True:
                batch.append(self._queue.get_nowait())
        except queue.Empty:
            pass
        if not batch:
            return
        try:
            httpx.post(self._ingest_url, json={"logs": batch}, timeout=5.0)
        except Exception:
            pass  # logging must never raise

    def _flush_loop(self) -> None:
        while not self._stop_event.wait(self._flush_interval):
            self._flush()
        self._flush()  # drain on shutdown

    def close(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=10)
        super().close()
