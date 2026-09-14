# http_api_contracts.operation_openapi

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-operation-openapi:a252904e7c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4c2a587616"></a>
- <a id="s-fe1f1b2364"></a>`distribution`: `http-api-contracts`
- <a id="s-03160b19ac"></a>`module`: `http_api_contracts`
- <a id="s-14ac23bc8f"></a>`name`: `operation_openapi`
- <a id="s-bc5f04789e"></a>`unit`: `export`

### Declared structure

- <a id="s-fc186d5fa6"></a>`kind`: `"function"`
- <a id="s-2093dc4f32"></a>`signature`: `"\"(contract: 'HttpOperationContract', *, error_type: 'object \| None' = None) -> 'dict[str, Any]'\""`

## Governing policies

- <a id="pa-e042f7a382"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.operation_openapi`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6a0ab40e3176ae915035a4db6d07922b5cc310a46013e0526a316d415d30b6a5 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(contract: 'HttpOperationContract', *, error_type: 'object | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "operation_openapi",
  "unit": "export"
}
```
