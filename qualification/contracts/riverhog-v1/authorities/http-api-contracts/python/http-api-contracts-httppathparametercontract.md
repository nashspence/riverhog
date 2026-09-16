# http_api_contracts.HttpPathParameterContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-httppathparametercontract:9511972423 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-76c0cb9671"></a>
- <a id="s-aa65265e83"></a>`distribution`: `http-api-contracts`
- <a id="s-3d3d563b23"></a>`module`: `http_api_contracts`
- <a id="s-9083d97508"></a>`name`: `HttpPathParameterContract`
- <a id="s-24d6c3e209"></a>`unit`: `export`

### Declared structure

- <a id="s-07ffca84ab"></a>`kind`: `"class"`
- <a id="s-bc98cc5b0b"></a>`signature`: `"\"(name: 'str', value_type: 'object') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-74d0e7f0e0"></a>`name` | `'str'` | `required` |
| <a id="s-bc487d005a"></a>`value_type` | `'object'` | `required` |

## Governing policies

- <a id="pa-505da5abaf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.HttpPathParameterContract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c59b52547880686dc5d35947a429f535f1440d7a69d97667d7630bcdc965916 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "name",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "value_type",
        "type": "'object'"
      }
    ],
    "kind": "class",
    "signature": "\"(name: 'str', value_type: 'object') -> None\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "HttpPathParameterContract",
  "unit": "export"
}
```

</details>
