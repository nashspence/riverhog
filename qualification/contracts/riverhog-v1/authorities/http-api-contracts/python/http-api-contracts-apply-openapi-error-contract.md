# http_api_contracts.apply_openapi_error_contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-apply-openapi-error-contract:6a0125bc3d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7c9fb97300"></a>
- <a id="s-5c688f11e0"></a>`distribution`: `http-api-contracts`
- <a id="s-e64cfb45d6"></a>`module`: `http_api_contracts`
- <a id="s-e01c96af6c"></a>`name`: `apply_openapi_error_contract`
- <a id="s-ff15409585"></a>`unit`: `export`

### Declared structure

- <a id="s-facb7b2426"></a>`kind`: `"function"`
- <a id="s-d044af7c86"></a>`signature`: `"\"(schema: 'dict[str, Any]', *, operation_error_authority: 'HttpOperationErrorAuthority \| None' = None) -> 'dict[str, Any]'\""`

## Governing policies

- <a id="pa-f533d51359"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.apply_openapi_error_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9c72b408633fa9e975a783a1774e7829b0b98598588ab7d74f5cda8ff84be231 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(schema: 'dict[str, Any]', *, operation_error_authority: 'HttpOperationErrorAuthority | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "apply_openapi_error_contract",
  "unit": "export"
}
```
