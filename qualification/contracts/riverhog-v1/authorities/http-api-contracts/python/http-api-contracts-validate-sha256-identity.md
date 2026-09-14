# http_api_contracts.validate_sha256_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-validate-sha256-identity:3c7ab9d95a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3932ac668b"></a>
- <a id="s-acdf6b5161"></a>`distribution`: `http-api-contracts`
- <a id="s-63ebc07811"></a>`module`: `http_api_contracts`
- <a id="s-d8bb008a40"></a>`name`: `validate_sha256_identity`
- <a id="s-cd51f7eccb"></a>`unit`: `export`

### Declared structure

- <a id="s-5f10247d9a"></a>`kind`: `"function"`
- <a id="s-8d76d32d52"></a>`signature`: `"\"(value: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-617944a443"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.validate_sha256_identity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7c3a5af682ee50e73ac777d234dc76d71edeeb8529e4a92bc1bd0cf77ebba307 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str') -> 'str'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "validate_sha256_identity",
  "unit": "export"
}
```
