# lifecycle_events.EventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-eventpage:8ee91e3310 -->

Exact externally visible contract owned by this contract element.

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
- <a id="s-8a9f74e0cb"></a>`signature`: `"'(*, events: list[lifecycle_events.models.LifecycleEvent], next_cursor: str, has_more: bool) -> None'"`

#### Validated model schema

<a id="s-0d4a6f7d8e"></a>

- <a id="s-fcff02be4e"></a>`type`: `"object"`
- <a id="s-f77f8098ad"></a>`additionalProperties`: `false`
- <a id="s-e604a856c3"></a>`required`: `["events","next_cursor","has_more"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0facf2cec7"></a>`events` | yes | type="array"; items=([LifecycleEvent](#s-ae9e965a9e)) |  |
| <a id="s-3c6245566d"></a>`has_more` | yes | type="boolean" |  |
| <a id="s-98ac6308d7"></a>`next_cursor` | yes | type="string" |  |

##### Definitions

- [LifecycleEvent](#s-ae9e965a9e)

##### <a id="s-ae9e965a9e"></a>definition `LifecycleEvent`

- <a id="s-a62e4de8b1"></a>`type`: `"object"`
- <a id="s-0759b74b7c"></a>`additionalProperties`: `false`
- <a id="s-e07ddc41ef"></a>`required`: `["id","type","occurred_at"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bd94225de7"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-bd291136e8"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-4bc03a082d"></a>`payload` | no | type="object"; additionalProperties=(any JSON value) |  |
| <a id="s-00ec5282e2"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-c4a8e7e2f7"></a>`type` | yes | type="string"; minLength=1 |  |

## Maintained corroboration

### Related interface records

- [require_progress_after](lifecycle-events-eventpage-require-progress-after.md)

## Governing policies

- <a id="pa-f0a223d5f6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources/authorities.md#src-7ab82f5e27) — [packages/lifecycle-events/src/lifecycle\_events/\_\_init\_\_.py](../../../../../../packages/lifecycle-events/src/lifecycle_events/__init__.py)

### Machine authority

- `/external_contract/python/lifecycle_events.EventPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 653e7d29a6c26d4f17263a38ac6256e436b2addb56ec017dfd95ee01cd94a478 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "LifecycleEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "additionalProperties": true,
              "type": "object"
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
            "type": {
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "occurred_at"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "events": {
          "items": {
            "$ref": "#/$defs/LifecycleEvent"
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
    "signature": "'(*, events: list[lifecycle_events.models.LifecycleEvent], next_cursor: str, has_more: bool) -> None'"
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "EventPage",
  "unit": "export"
}
```

</details>
