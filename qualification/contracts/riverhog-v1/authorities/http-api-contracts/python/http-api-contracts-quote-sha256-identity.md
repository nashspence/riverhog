# http_api_contracts.quote_sha256_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-quote-sha256-identity:b820644681 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-db0a2f1b44"></a>
- <a id="s-06aa40e787"></a>`distribution`: `http-api-contracts`
- <a id="s-c6a31a043e"></a>`module`: `http_api_contracts`
- <a id="s-39d2b4e0fa"></a>`name`: `quote_sha256_identity`
- <a id="s-7f5e0e59d0"></a>`unit`: `export`

### Declared structure

- <a id="s-0b69791b8f"></a>`kind`: `"function"`
- <a id="s-a90c8d781f"></a>`signature`: `"\"(value: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-e0ac35b36f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.quote_sha256_identity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0b49047c122e36510089684f89765e1f430b7239e27cc041299aec83f18f9125 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str') -> 'str'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "quote_sha256_identity",
  "unit": "export"
}
```
