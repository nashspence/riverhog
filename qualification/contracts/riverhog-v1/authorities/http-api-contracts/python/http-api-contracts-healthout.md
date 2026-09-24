# http_api_contracts.HealthOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-healthout:32e642714f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-079f05ec4e"></a>
- <a id="s-622e921c38"></a>`distribution`: `http-api-contracts`
- <a id="s-458c0662a4"></a>`module`: `http_api_contracts`
- <a id="s-2fe5c2be88"></a>`name`: `HealthOut`
- <a id="s-228c9f81b2"></a>`unit`: `export`

### Declared structure

- <a id="s-83602a7ef1"></a>`kind`: `"class"`
- <a id="s-4754f1f571"></a>`signature`: `"\"(*, service: Annotated[str, MinLen(min_length=1)], status: Literal['ok']) -> None\""`

#### Validated model schema

<a id="s-cada66025a"></a>

- <a id="s-3c32cc8673"></a>`type`: `"object"`
- <a id="s-299b46003c"></a>`additionalProperties`: `false`
- <a id="s-53f00b7350"></a>`required`: `["service","status"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dddde3cc5a"></a>`service` | yes | type="string"; minLength=1 |  |
| <a id="s-1cc1c8e23d"></a>`status` | yes | type="string"; const="ok" |  |

## Governing policies

- <a id="pa-9b4f91118e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources/authorities.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.HealthOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b5c4a13ab7dcaceed96265fe6ce33452dd2a1b9240b5c573a9b0997aa199deeb -->

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
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "HealthOut",
  "unit": "export"
}
```

</details>
