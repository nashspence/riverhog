# lifecycle_events.CloudEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:lifecycle-events:lifecycle-events-cloudevent:3795582d07 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-ea7ddd3dda"></a>`signature`: `"\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Annotated[str, MinLen(min_length=1)], type: Annotated[str, MinLen(min_length=1)], subject: Annotated[str \| None, MinLen(min_length=1)] = None, time: str, datacontenttype: Literal['application/json'] = 'application/json', data: dict[str, typing.Any] = <factory>) -> None\""`

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
| <a id="s-687ecd37d4"></a>`time` | yes | type="string" |  |
| <a id="s-45e923bd2e"></a>`type` | yes | type="string"; minLength=1 |  |

## Maintained corroboration

### Related interface records

- [validate_time](lifecycle-events-cloudevent-validate-time.md)

## Governing policies

- <a id="pa-b731b7c842"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:lifecycle-events:lifecycle_events](../../../evidence/sources.md#src-7ab82f5e27) — `packages/lifecycle-events/src/lifecycle_events/__init__.py`

### Machine authority

- `/external_contract/python/lifecycle_events.CloudEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ccbd62dc77c4b42d31bb62e5c5630c3203ee2aba228af4a43261274b62d8292f -->

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
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Annotated[str, MinLen(min_length=1)], type: Annotated[str, MinLen(min_length=1)], subject: Annotated[str | None, MinLen(min_length=1)] = None, time: str, datacontenttype: Literal['application/json'] = 'application/json', data: dict[str, typing.Any] = <factory>) -> None\""
  },
  "distribution": "lifecycle-events",
  "module": "lifecycle_events",
  "name": "CloudEvent",
  "unit": "export"
}
```

</details>
