# lifecycle_events.LifecycleEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-lifecycleevent:1d1740635f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2c7caa91db"></a>
- <a id="s-c7c74f2774"></a>`distribution`: `lifecycle-events`
- <a id="s-e4b9a3e04a"></a>`module`: `lifecycle_events`
- <a id="s-8fbc98cfd8"></a>`name`: `LifecycleEvent`
- <a id="s-debbe6c8ce"></a>`unit`: `export`

### Declared structure

- <a id="s-2f9ec32e6e"></a>`kind`: `"class"`
- <a id="s-041f2461e5"></a>`signature`: `"\"(*, id: Annotated[str, MinLen(min_length=1)], type: Annotated[str, MinLen(min_length=1)], subject: Annotated[str \| None, MinLen(min_length=1)] = None, occurred_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)], payload: dict[str, typing.Any] = <factory>) -> None\""`

#### Validated model schema

<a id="s-c2b9e1dec3"></a>

- <a id="s-af763806e9"></a>`type`: `"object"`
- <a id="s-3f6a2d83cf"></a>`additionalProperties`: `false`
- <a id="s-f0848b3d6f"></a>`required`: `["id","type","occurred_at"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2d0add86d3"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-a0b731615b"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-977e0f87a4"></a>`payload` | no | type="object"; additionalProperties=(any JSON value) |  |
| <a id="s-01b75fd2d3"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-cf537049ee"></a>`type` | yes | type="string"; minLength=1 |  |

## Governing policies

- <a id="pa-3aa84c58f5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources/authorities.md#src-7ab82f5e27) — [packages/lifecycle-events/src/lifecycle\_events/\_\_init\_\_.py](../../../../../../packages/lifecycle-events/src/lifecycle_events/__init__.py)

### Machine authority

- `/external_contract/python/lifecycle_events.LifecycleEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 077a0c04c497b66888d0ce87232f06502368f01087bb4e1d7de25e6ea1948a86 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
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
    },
    "signature": "\"(*, id: Annotated[str, MinLen(min_length=1)], type: Annotated[str, MinLen(min_length=1)], subject: Annotated[str | None, MinLen(min_length=1)] = None, occurred_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)], payload: dict[str, typing.Any] = <factory>) -> None\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "LifecycleEvent",
  "unit": "export"
}
```

</details>
