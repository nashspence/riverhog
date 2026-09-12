# lifecycle_events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events:eac5f60a5d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-b6f5a432e078) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-79ad8bdd322d"></a>
| Field | Shape |
|---|---|
| <a id="s-2bab04a37341"></a>`distribution` | "lifecycle-events" |
| <a id="s-4c9c1968650e"></a>`exports` | additional keys=`CLOUDEVENTS_JSON_CONTENT_TYPE`, `CloudEvent`, `EventContext`, `EventPage`, `LifecycleEventClient`, `MAX_EVENT_CONTEXT_BYTES`, `SQLiteEventCursorStore`, `SQLiteLifecycleEventLog`, `caused_event`, `cloud_event`, `create_lifecycle_event_schema`, `normalize_event_context` |
| <a id="s-2a0e3d06dea2"></a>`module` | "lifecycle_events" |

## Governing policies

- <a id="pa-ab76c84034ed"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba506)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events](../../../evidence/sources.md#src-feb6b7ae7098) — `packages/lifecycle-events/src/lifecycle_events/__init__.py::<module>`

### Machine authority

- `/external_contract/python/1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4d287dd1a47decf294ed3ca4223dbd5292015376ff5148180676bf2890491805 -->

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
