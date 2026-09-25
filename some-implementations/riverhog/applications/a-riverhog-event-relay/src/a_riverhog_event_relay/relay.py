from __future__ import annotations

import json
import logging
import sqlite3
import threading
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from config_validation import load_validated_yaml_config, read_secret_file
from lifecycle_events.models import EventPage, LifecycleEvent
from pydantic import BaseModel, ConfigDict, Field, field_validator

from a_riverhog_event_relay.schema import validate_state

LOG = logging.getLogger(__name__)
CLOUDEVENTS_JSON_CONTENT_TYPE = "application/cloudevents+json"


def cloud_event_document(event: LifecycleEvent, *, source_name: str) -> dict[str, Any]:
    """Translate a native event at the outbound webhook boundary."""
    document: dict[str, Any] = {
        "specversion": "1.0",
        "id": event.id,
        "source": f"urn:riverhog:event-source:{source_name}",
        "type": event.type,
        "time": event.occurred_at,
        "datacontenttype": "application/json",
        "data": event.payload,
    }
    if event.subject is not None:
        document["subject"] = event.subject
    return document


@dataclass(frozen=True)
class SourceFailure:
    source: str
    phase: str
    error_type: str
    status_code: int | None


@dataclass(frozen=True)
class RunOnceResult:
    delivered: int
    failures: tuple[SourceFailure, ...]


class SourceRelayError(RuntimeError):
    """A secret-free description of one failed source relay phase."""

    def __init__(self, *, phase: str, error_type: str, status_code: int | None) -> None:
        self.phase = phase
        self.error_type = error_type
        self.status_code = status_code
        status = f" status={status_code}" if status_code is not None else ""
        super().__init__(f"{phase} failed: error={error_type}{status}")


def _relay_error(phase: str, error: Exception) -> SourceRelayError:
    status_code = error.response.status_code if isinstance(error, httpx.HTTPStatusError) else None
    return SourceRelayError(
        phase=phase,
        error_type=type(error).__name__,
        status_code=status_code,
    )


def _source_failure(source: SourceConfig, error: Exception) -> SourceFailure:
    if isinstance(error, SourceRelayError):
        return SourceFailure(
            source=source.name,
            phase=error.phase,
            error_type=error.error_type,
            status_code=error.status_code,
        )
    return SourceFailure(
        source=source.name,
        phase="relay",
        error_type=type(error).__name__,
        status_code=None,
    )


def _log_failure(failure: SourceFailure) -> None:
    LOG.error(
        "Riverhog event relay source failed: source=%s phase=%s error=%s status=%s",
        failure.source,
        failure.phase,
        failure.error_type,
        failure.status_code if failure.status_code is not None else "none",
    )


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, pattern=r"^[A-Za-z0-9._-]+$")
    events_url: str = Field(min_length=1)
    token_file: Path
    webhook_url_file: Path


class EventRelayConfig(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        json_schema_extra={
            "$id": "https://nashspence.github.io/riverhog/v1/config/a-riverhog-event-relay.schema.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
        },
    )

    version: int = 1
    state_path: Path
    poll_interval_seconds: float = Field(default=5.0, gt=0)
    request_timeout_seconds: float = Field(default=10.0, gt=0)
    batch_size: int = Field(default=100, ge=1, le=100)
    sources: tuple[SourceConfig, ...] = Field(min_length=1)

    @field_validator("version")
    @classmethod
    def validate_version(cls, value: int) -> int:
        if value != 1:
            raise ValueError("Riverhog event relay config version must be 1")
        return value

    @field_validator("sources")
    @classmethod
    def unique_sources(cls, value: tuple[SourceConfig, ...]) -> tuple[SourceConfig, ...]:
        names = [source.name for source in value]
        if len(names) != len(set(names)):
            raise ValueError("Riverhog event relay source names must be unique")
        return value


def load_config(path: Path) -> EventRelayConfig:
    config_path = path.expanduser().resolve()
    schema = json.loads(Path(__file__).with_name("config.schema.json").read_text(encoding="utf-8"))
    payload = load_validated_yaml_config(config_path, schema)
    config = EventRelayConfig.model_validate(payload)
    state_path = config.state_path.expanduser()
    if not state_path.is_absolute():
        state_path = config_path.parent / state_path
    config = config.model_copy(update={"state_path": state_path.resolve()})
    for source in config.sources:
        read_secret_file(source.token_file, label=f"source {source.name} token_file")
        read_secret_file(source.webhook_url_file, label=f"source {source.name} webhook_url_file")
    return config


def generated_config_schema() -> dict[str, object]:
    return EventRelayConfig.model_json_schema()


class CursorState:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()

    def validate(self) -> None:
        validate_state(self.path)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(f"{self.path.as_uri()}?mode=rw", uri=True, timeout=30)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def cursor(self, source: str) -> str:
        with self._lock, closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT cursor FROM source_cursors WHERE source = ?", (source,)
            ).fetchone()
        return str(row[0]) if row is not None else "0"

    def advance(self, source: str, cursor: str) -> None:
        with self._lock, closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT INTO source_cursors(source, cursor) VALUES(?, ?)
                ON CONFLICT(source) DO UPDATE SET cursor = excluded.cursor
                """,
                (source, cursor),
            )
            connection.commit()


class EventRelay:
    def __init__(self, config: EventRelayConfig) -> None:
        self.config = config
        self.state = CursorState(config.state_path)
        self.state.validate()

    def relay_source_once(
        self,
        source: SourceConfig,
        *,
        client: httpx.Client | None = None,
    ) -> int:
        owns_client = client is None
        http = client or httpx.Client(timeout=self.config.request_timeout_seconds)
        try:
            try:
                cursor = self.state.cursor(source.name)
            except Exception as exc:
                raise _relay_error("state", exc) from None
            try:
                response = http.get(
                    source.events_url,
                    params={"after": cursor, "limit": self.config.batch_size},
                    headers={
                        "Authorization": "Bearer "
                        + read_secret_file(
                            source.token_file, label=f"source {source.name} token_file"
                        )
                    },
                )
                response.raise_for_status()
                page = EventPage.model_validate(response.json())
                page.require_progress_after(cursor)
            except Exception as exc:
                raise _relay_error("fetch", exc) from None
            delivered = 0
            for event in page.events:
                try:
                    delivery = http.post(
                        read_secret_file(
                            source.webhook_url_file,
                            label=f"source {source.name} webhook_url_file",
                        ),
                        content=json.dumps(
                            cloud_event_document(event, source_name=source.name),
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        headers={"Content-Type": CLOUDEVENTS_JSON_CONTENT_TYPE},
                    )
                    delivery.raise_for_status()
                except Exception as exc:
                    raise _relay_error("delivery", exc) from None
                delivered += 1
            if page.events or page.next_cursor != cursor:
                try:
                    self.state.advance(source.name, page.next_cursor)
                except Exception as exc:
                    raise _relay_error("state", exc) from None
            return delivered
        finally:
            if owns_client:
                http.close()

    def run_once(self) -> RunOnceResult:
        delivered = 0
        failures: list[SourceFailure] = []
        with httpx.Client(timeout=self.config.request_timeout_seconds) as client:
            for source in self.config.sources:
                try:
                    delivered += self.relay_source_once(source, client=client)
                except Exception as exc:
                    failure = _source_failure(source, exc)
                    failures.append(failure)
                    _log_failure(failure)
        return RunOnceResult(delivered=delivered, failures=tuple(failures))

    def run(self) -> None:
        stop = threading.Event()
        threads = [
            threading.Thread(
                target=self._run_source,
                args=(source, stop),
                name=f"a-riverhog-event-relay-{source.name}",
                daemon=False,
            )
            for source in self.config.sources
        ]
        for thread in threads:
            thread.start()
        try:
            for thread in threads:
                thread.join()
        except KeyboardInterrupt:
            stop.set()
            for thread in threads:
                thread.join(timeout=self.config.request_timeout_seconds + 1)

    def _run_source(self, source: SourceConfig, stop: threading.Event) -> None:
        with httpx.Client(timeout=self.config.request_timeout_seconds) as client:
            while not stop.is_set():
                try:
                    delivered = self.relay_source_once(source, client=client)
                    if delivered:
                        continue
                except Exception as exc:
                    _log_failure(_source_failure(source, exc))
                stop.wait(self.config.poll_interval_seconds)


def summarize_config(config: EventRelayConfig) -> dict[str, Any]:
    return {
        "state_path": str(config.state_path),
        "sources": [source.name for source in config.sources],
        "poll_interval_seconds": config.poll_interval_seconds,
        "request_timeout_seconds": config.request_timeout_seconds,
        "batch_size": config.batch_size,
    }


__all__ = [
    "EventRelay",
    "EventRelayConfig",
    "SourceConfig",
    "load_config",
    "summarize_config",
]
