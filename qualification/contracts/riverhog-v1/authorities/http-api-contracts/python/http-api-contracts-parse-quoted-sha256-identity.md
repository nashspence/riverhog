# http_api_contracts.parse_quoted_sha256_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-parse-quoted-sha256-identity:38df9fbe4b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3613d650b1"></a>
| Field | Shape |
|---|---|
| <a id="s-f69d2e0b2f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-f37ba9cadc"></a>`distribution` | "http-api-contracts" |
| <a id="s-2ed220e6da"></a>`module` | "http_api_contracts" |
| <a id="s-ee6311722b"></a>`name` | "parse_quoted_sha256_identity" |
| <a id="s-fb6ce37629"></a>`unit` | "export" |

## Governing policies

- <a id="pa-3c72fcd8f6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.parse_quoted_sha256_identity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7fc089e1fdf2d922b123d4bdc03c1e753ed80dbb78951724b9f2bb1ff1017e5f -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str') -> 'str'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "parse_quoted_sha256_identity",
  "unit": "export"
}
```
