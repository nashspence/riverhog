# stove0_operator_contracts.STOVE0_HTTP_ERROR_AUTHORITY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0-http-err-50f2e6f73d:b36485f3e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e3ac4c2f98"></a>
| Field | Shape |
|---|---|
| <a id="s-795a484790"></a>`contract` | type="http_api_contracts.HttpOperationErrorAuthority"; additional keys=`kind` |
| <a id="s-6bef9e6620"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-126605e807"></a>`module` | "stove0_operator_contracts" |
| <a id="s-40aae4ccc9"></a>`name` | "STOVE0_HTTP_ERROR_AUTHORITY" |
| <a id="s-6a6e304c98"></a>`unit` | "export" |

## Governing policies

- <a id="pa-2edd45f215"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.STOVE0_HTTP_ERROR_AUTHORITY`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 291b656f43d04959496f1e51725b25d335723b8a867e6842b522fefea134f8ee -->

```json
{
  "contract": {
    "kind": "object",
    "type": "http_api_contracts.HttpOperationErrorAuthority"
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "STOVE0_HTTP_ERROR_AUTHORITY",
  "unit": "export"
}
```
