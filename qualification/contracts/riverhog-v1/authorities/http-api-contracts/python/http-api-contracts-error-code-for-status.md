# http_api_contracts.error_code_for_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-error-code-for-status:cb7eaa89a3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c2c07ab009"></a>
| Field | Shape |
|---|---|
| <a id="s-fea6f472c1"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ff033c241e"></a>`distribution` | "http-api-contracts" |
| <a id="s-7fadb9a705"></a>`module` | "http_api_contracts" |
| <a id="s-9b774b00f7"></a>`name` | "error_code_for_status" |
| <a id="s-7b2d8b269f"></a>`unit` | "export" |

## Governing policies

- <a id="pa-fd0babd3af"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.error_code_for_status`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f41e27607994ff73682d38f8494f84d20e00392005c6245c011cba6845fd67a1 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(status: 'int') -> 'str'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "error_code_for_status",
  "unit": "export"
}
```
