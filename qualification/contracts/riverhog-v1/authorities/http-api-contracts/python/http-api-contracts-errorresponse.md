# http_api_contracts.ErrorResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-errorresponse:62c8457bf7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-91b204b1ab"></a>
| Field | Shape |
|---|---|
| <a id="s-8e99f32aba"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-40574b2b62"></a>`distribution` | "http-api-contracts" |
| <a id="s-2848a82eae"></a>`module` | "http_api_contracts" |
| <a id="s-10efc4a45b"></a>`name` | "ErrorResponse" |
| <a id="s-860b8705fd"></a>`unit` | "export" |

## Governing policies

- <a id="pa-78b812f1e8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.ErrorResponse`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5cf688b23ae41757f21715b9ab51108b320f636ae6b2026017c4287735e0d5bf -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d60b687f27e4869742bff79f639b284dc47aeb1f716d9e75178e79b171ac4f9f",
    "signature": "'(*, error: http_api_contracts.ErrorBody) -> None'"
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "ErrorResponse",
  "unit": "export"
}
```
