# http_api_contracts.HealthResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-healthresponse:15b925f3ec -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c086949aac"></a>
- <a id="s-61740d85b0"></a>`distribution`: `http-api-contracts`
- <a id="s-a0ee19967c"></a>`module`: `http_api_contracts`
- <a id="s-38e35b295a"></a>`name`: `HealthResponse`
- <a id="s-3898c814ba"></a>`unit`: `export`

### Declared structure

- <a id="s-32679821a8"></a>`kind`: `"class"`
- <a id="s-a75875effc"></a>`signature`: `"\"(*, service: Annotated[str, MinLen(min_length=1)], status: Literal['ok']) -> None\""`

#### Validated model schema

<a id="s-a6a5fbcb59"></a>

- <a id="s-c36814c318"></a>`type`: `"object"`
- <a id="s-f53dcaa250"></a>`additionalProperties`: `false`
- <a id="s-190dd119a5"></a>`required`: `["service","status"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1a5a243fe3"></a>`service` | yes | type="string"; minLength=1 |  |
| <a id="s-aa328af297"></a>`status` | yes | type="string"; const="ok" |  |

## Governing policies

- <a id="pa-9a3e812c6a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.HealthResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9b3873e483fc1c875df0c78f3e607f09bbae9dc45d1533e5b844b6aca3050a11 -->

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
  "name": "HealthResponse",
  "unit": "export"
}
```

</details>
