# lifecycle_events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events:eac5f60a5d -->

| Audit field | Value |
|---|---|
| Authority | `lifecycle-events` |
| Interface | `python` |
| Family | `modules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/python/1`

## Effective policies

- `compatibility/python-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `python:lifecycle-events` — `packages/lifecycle-events/src/lifecycle_events/__init__.py::<module>`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract

```json
{
  "distribution": "lifecycle-events",
  "exports": {
    "CLOUDEVENTS_JSON_CONTENT_TYPE": {
      "kind": "constant",
      "value": "application/cloudevents+json"
    },
    "CloudEvent": {
      "kind": "class",
      "members": {
        "validate_time": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        }
      },
      "schema_sha256": "4f1bdc2cc9c131a7c8aad0b1bbe56f7bf3cf1f2c4a12a0727d577333cfc771c8",
      "signature": "(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Annotated[str, MinLen(min_length=1)], type: Annotated[str, MinLen(min_length=1)], subject: Annotated[str | None, MinLen(min_length=1)] = None, time: str, datacontenttype: Literal['application/json'] = 'application/json', data: dict[str, typing.Any] = <factory>) -> None"
    },
    "EventContext": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
    },
    "EventPage": {
      "kind": "class",
      "members": {
        "require_progress_after": {
          "kind": "method",
          "signature": "(self, cursor: 'str') -> 'None'"
        }
      },
      "schema_sha256": "8847c9798649b25d96f5d871b15ead3ffee87f978f685526fc5b91476d6a03e9",
      "signature": "(*, events: list[lifecycle_events.models.CloudEvent], next_cursor: str, has_more: bool) -> None"
    },
    "LifecycleEventClient": {
      "kind": "class",
      "members": {
        "close": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "page": {
          "kind": "method",
          "signature": "(self, *, after: 'str | None', limit: 'int' = 100) -> 'EventPage'"
        }
      },
      "signature": "(events_url: 'str', *, token: 'str', timeout: 'float' = 10.0, client: 'httpx.Client | None' = None) -> 'None'"
    },
    "MAX_EVENT_CONTEXT_BYTES": {
      "kind": "constant",
      "value": 4096
    },
    "SQLiteEventCursorStore": {
      "kind": "class",
      "members": {
        "advance": {
          "kind": "method",
          "signature": "(self, source: 'str', cursor: 'str') -> 'None'"
        },
        "cursor": {
          "kind": "method",
          "signature": "(self, source: 'str') -> 'str'"
        },
        "initialize": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        }
      },
      "signature": "(connect: 'Callable[[], sqlite3.Connection]') -> 'None'"
    },
    "SQLiteLifecycleEventLog": {
      "kind": "class",
      "members": {
        "append": {
          "kind": "method",
          "signature": "(self, event: 'CloudEvent', *, owner: 'str', context: 'dict[str, object] | None' = None, context_expires_at: 'str | None' = None) -> 'int'"
        },
        "append_once": {
          "kind": "method",
          "signature": "(self, event: 'CloudEvent', *, owner: 'str', context: 'dict[str, object] | None' = None, context_expires_at: 'str | None' = None) -> 'int'"
        },
        "expire_context": {
          "kind": "method",
          "signature": "(self, *, owner: 'str', subject: 'str', expires_at: 'str') -> 'int'"
        },
        "initialize": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "page": {
          "kind": "method",
          "signature": "(self, *, after: 'str | None', limit: 'int', owner: 'str | None' = None) -> 'EventPage'"
        }
      },
      "signature": "(connect: 'Callable[[], sqlite3.Connection]') -> 'None'"
    },
    "caused_event": {
      "kind": "function",
      "signature": "(*, cause: 'CloudEvent', source: 'str', type: 'str', data: 'Mapping[str, Any] | None' = None, subject: 'str | None' = None, occurred_at: 'datetime | None' = None) -> 'CloudEvent'"
    },
    "cloud_event": {
      "kind": "function",
      "signature": "(*, source: 'str', type: 'str', data: 'Mapping[str, Any] | None' = None, subject: 'str | None' = None, occurred_at: 'datetime | None' = None, event_id: 'str | None' = None) -> 'CloudEvent'"
    },
    "create_lifecycle_event_schema": {
      "kind": "function",
      "signature": "(connection: 'sqlite3.Connection') -> 'None'"
    },
    "normalize_event_context": {
      "kind": "function",
      "signature": "(value: 'Mapping[str, Any] | None', *, max_bytes: 'int' = 4096) -> 'dict[str, Any] | None'"
    }
  },
  "module": "lifecycle_events"
}
```
