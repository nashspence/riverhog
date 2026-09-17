# http_api_contracts.validate_browse_query

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-validate-browse-query:99c346e82b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fe7a7a2a22"></a>
- <a id="s-23f720254b"></a>`distribution`: `http-api-contracts`
- <a id="s-d16e17078c"></a>`module`: `http_api_contracts`
- <a id="s-105851d8ff"></a>`name`: `validate_browse_query`
- <a id="s-238719a58d"></a>`unit`: `export`

### Declared structure

- <a id="s-53b4597b56"></a>`kind`: `"function"`
- <a id="s-8f9f95e015"></a>`signature`: `"\"(value: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-93e2e3d4c2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.validate_browse_query`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ee48fb8ed3781149247e17104c2e66d2ec1c683ce43d4d27b4eb67ce1c50f36a -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str') -> 'str'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "validate_browse_query",
  "unit": "export"
}
```

</details>
