# stove0_api_client.HealthOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-healthout:c7a3bb5244 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-00308a44d0"></a>
- <a id="s-52482e97ad"></a>`distribution`: `stove0-api-client`
- <a id="s-06fc0e1dd3"></a>`module`: `stove0_api_client`
- <a id="s-4808063689"></a>`name`: `HealthOut`
- <a id="s-48f59946a9"></a>`unit`: `export`

### Declared structure

- <a id="s-4d6da632a8"></a>`kind`: `"class"`
- <a id="s-0b44bef8ce"></a>`signature`: `"\"(*, service: Annotated[str, MinLen(min_length=1)], status: Literal['ok']) -> None\""`

#### Validated model schema

<a id="s-ed5b36bf4a"></a>

- <a id="s-c239721f85"></a>`type`: `"object"`
- <a id="s-5b2abfcee0"></a>`additionalProperties`: `false`
- <a id="s-b70751a387"></a>`required`: `["service","status"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d800a35e4f"></a>`service` | yes | type="string"; minLength=1 |  |
| <a id="s-9d03fc6ef6"></a>`status` | yes | type="string"; const="ok" |  |

## Governing policies

- <a id="pa-ede693620b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources/authorities.md#src-5d52ac5998) — [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/__init__.py)

### Machine authority

- `/external_contract/python/stove0_api_client.HealthOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c4cc9183ea40dfea00881fffd6a2435a172bb9e9a150313f7f59e9f5135aa9f6 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "service": {
          "minLength": 1,
          "type": "string"
        },
        "status": {
          "const": "ok",
          "type": "string"
        }
      },
      "required": [
        "service",
        "status"
      ],
      "type": "object"
    },
    "signature": "\"(*, service: Annotated[str, MinLen(min_length=1)], status: Literal['ok']) -> None\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "HealthOut",
  "unit": "export"
}
```

</details>
