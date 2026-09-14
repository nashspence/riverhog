# http_api_contracts.exact_authority_page_operation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-exact-authority-page-operation:19ebe48f15 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-24c56714ba"></a>
- <a id="s-4b2588726b"></a>`distribution`: `http-api-contracts`
- <a id="s-59dd53af32"></a>`module`: `http_api_contracts`
- <a id="s-19d068e6de"></a>`name`: `exact_authority_page_operation`
- <a id="s-f9a539142e"></a>`unit`: `export`

### Declared structure

- <a id="s-24a4479020"></a>`kind`: `"function"`
- <a id="s-bc783d126b"></a>`signature`: `"\"(*, authority: 'str', authority_parameter: 'str \| None', cursor_parameter: 'str', limit_parameter: 'str \| None' = None, fixed_limit: 'int \| None' = None) -> 'dict[str, Any]'\""`

## Governing policies

- <a id="pa-3bc417b3c4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.exact_authority_page_operation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 28e381c96e50b9cd28afb1355e422d7d466874ceeaeee06cb2a4718a1ea4a9c6 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, authority: 'str', authority_parameter: 'str | None', cursor_parameter: 'str', limit_parameter: 'str | None' = None, fixed_limit: 'int | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "exact_authority_page_operation",
  "unit": "export"
}
```
