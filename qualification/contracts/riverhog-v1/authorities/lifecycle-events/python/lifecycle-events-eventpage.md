# lifecycle_events.EventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-eventpage:8ee91e3310 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-97fc9498e1"></a>
- <a id="s-97815d04e4"></a>`distribution`: `lifecycle-events`
- <a id="s-88319aa5d7"></a>`module`: `lifecycle_events`
- <a id="s-7ff3a644d0"></a>`name`: `EventPage`
- <a id="s-b44473440b"></a>`unit`: `export`

### Declared structure

- <a id="s-b00e398e69"></a>`kind`: `"class"`
- <a id="s-8a9f74e0cb"></a>`signature`: `"'(*, events: list[lifecycle_events.models.CloudEvent], next_cursor: str, has_more: bool) -> None'"`

#### Validated model schema

<a id="s-0d4a6f7d8e"></a>
- <a id="s-fcff02be4e"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0facf2cec7"></a>`events` | yes | type="array"; items=(#/$defs/CloudEvent) |  |
| <a id="s-3c6245566d"></a>`has_more` | yes | type="boolean" |  |
| <a id="s-98ac6308d7"></a>`next_cursor` | yes | type="string" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-d72037c6f5"></a>`CloudEvent` | type="object"; fields=`data`, `datacontenttype`, `id`, `source`, `specversion`, `subject`, `time`, `type`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [lifecycle_events.EventPage.require_progress_after](lifecycle-events-eventpage-require-progress-after.md)

## Governing policies

- <a id="pa-f0a223d5f6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.EventPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7b19ed6d9d85c8f889a4c2d44b56f95afa99c46d21ba3585950d233d72dcb0d2 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CloudEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "additionalProperties": true,
              "type": "object"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "time": {
              "type": "string"
            },
            "type": {
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "events": {
          "items": {
            "$ref": "#/$defs/CloudEvent"
          },
          "type": "array"
        },
        "has_more": {
          "type": "boolean"
        },
        "next_cursor": {
          "type": "string"
        }
      },
      "required": [
        "events",
        "next_cursor",
        "has_more"
      ],
      "type": "object"
    },
    "signature": "'(*, events: list[lifecycle_events.models.CloudEvent], next_cursor: str, has_more: bool) -> None'"
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "EventPage",
  "unit": "export"
}
```
