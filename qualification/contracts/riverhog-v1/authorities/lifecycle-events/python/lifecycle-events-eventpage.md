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

- <a id="s-fcff02be4e"></a>`type`: `"object"`
- <a id="s-f77f8098ad"></a>`additionalProperties`: `false`
- <a id="s-e604a856c3"></a>`required`: `["events","next_cursor","has_more"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0facf2cec7"></a>`events` | yes | type="array"; items=([CloudEvent](#s-d72037c6f5)) |  |
| <a id="s-3c6245566d"></a>`has_more` | yes | type="boolean" |  |
| <a id="s-98ac6308d7"></a>`next_cursor` | yes | type="string" |  |

##### Definitions

- [CloudEvent](#s-d72037c6f5)

##### <a id="s-d72037c6f5"></a>definition `CloudEvent`

- <a id="s-f10e2d53a4"></a>`type`: `"object"`
- <a id="s-af2d35022e"></a>`additionalProperties`: `false`
- <a id="s-f6c00b9e7d"></a>`required`: `["id","source","type","time"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-59024638e2"></a>`data` | no | type="object"; additionalProperties=true |  |
| <a id="s-0f0e8ae15b"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-84846940b3"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-d690335fef"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-5dfa018c91"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-cf47bcc831"></a>`subject` | no | anyOf=(type="string"; minLength=1) \| (type="null"); default=null |  |
| <a id="s-d33f524596"></a>`time` | yes | type="string" |  |
| <a id="s-206b110aaf"></a>`type` | yes | type="string"; minLength=1 |  |

## Maintained corroboration

### Related interface records

- [require_progress_after](lifecycle-events-eventpage-require-progress-after.md)

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

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
