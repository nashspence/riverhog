# http_api_contracts.status_for_error_code

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-status-for-error-code:cd71f351f7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9901bd9596"></a>
| Field | Shape |
|---|---|
| <a id="s-87805a0e69"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-1c5447a837"></a>`distribution` | "http-api-contracts" |
| <a id="s-e3d8dc167d"></a>`module` | "http_api_contracts" |
| <a id="s-db6b38608e"></a>`name` | "status_for_error_code" |
| <a id="s-917aedca73"></a>`unit` | "export" |

## Governing policies

- <a id="pa-c25e576605"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.status_for_error_code`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 762f86cdb3558c932566a1a44a8c32eda59e957161b0732dd2ad70ba570cf666 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(code: 'str', *, fallback: 'int' = 500) -> 'int'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "status_for_error_code",
  "unit": "export"
}
```
