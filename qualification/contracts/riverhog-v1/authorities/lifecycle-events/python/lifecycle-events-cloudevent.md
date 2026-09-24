# lifecycle_events.CloudEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-cloudevent:3795582d07 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [lifecycle-events](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1177f7ab8d"></a>
- <a id="s-a7b4f3e4bc"></a>`distribution`: `lifecycle-events`
- <a id="s-ad7bf5ebbf"></a>`module`: `lifecycle_events`
- <a id="s-2f03e10a66"></a>`name`: `CloudEvent`
- <a id="s-19c004df0d"></a>`unit`: `export`

### Declared structure

- <a id="s-b267ee0cbf"></a>`kind`: `"class"`
- <a id="s-ea7ddd3dda"></a>`signature`: `"\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Annotated[str, MinLen(min_length=1)], type: Annotated[str, MinLen(min_length=1)], subject: Annotated[str \| None, MinLen(min_length=1)] = None, time: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)], datacontenttype: Literal['application/json'] = 'application/json', data: dict[str, typing.Any] = <factory>) -> None\""`

#### Validated model schema

<a id="s-1b227bba88"></a>

- <a id="s-a1faf51260"></a>`type`: `"object"`
- <a id="s-e11e44fecc"></a>`additionalProperties`: `false`
- <a id="s-8446f2be64"></a>`required`: `["id","source","type","time"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bdab0ff150"></a>`data` | no | type="object"; additionalProperties=(any JSON value) |  |
| <a id="s-ce18951641"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-fe5b598876"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-ffcf800cd2"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-6194936dee"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-722293f09c"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-687ecd37d4"></a>`time` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-45e923bd2e"></a>`type` | yes | type="string"; minLength=1 |  |

## Governing policies

- <a id="pa-b731b7c842"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources/authorities.md#src-7ab82f5e27) — [packages/lifecycle-events/src/lifecycle\_events/\_\_init\_\_.py](../../../../../../packages/lifecycle-events/src/lifecycle_events/__init__.py)

### Machine authority

- `/external_contract/python/lifecycle_events.CloudEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ca4fb8a0fd050472b7dd6dbf76582e971bc0b6a0dbf937352f8fed4360596b78 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
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
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
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
    },
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Annotated[str, MinLen(min_length=1)], type: Annotated[str, MinLen(min_length=1)], subject: Annotated[str | None, MinLen(min_length=1)] = None, time: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)], datacontenttype: Literal['application/json'] = 'application/json', data: dict[str, typing.Any] = <factory>) -> None\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "CloudEvent",
  "unit": "export"
}
```

</details>
