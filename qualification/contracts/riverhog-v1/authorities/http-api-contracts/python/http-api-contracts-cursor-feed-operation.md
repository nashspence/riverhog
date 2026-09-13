# http_api_contracts.cursor_feed_operation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-cursor-feed-operation:d5845a93ca -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d7080ab710"></a>
| Field | Shape |
|---|---|
| <a id="s-a9a526e922"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-8ecf733101"></a>`distribution` | "http-api-contracts" |
| <a id="s-b2c1b576df"></a>`module` | "http_api_contracts" |
| <a id="s-f993f14543"></a>`name` | "cursor_feed_operation" |
| <a id="s-0845f65ce8"></a>`unit` | "export" |

## Governing policies

- <a id="pa-8f89e6a98e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.cursor_feed_operation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7f693b232243878e011baf2fa5d2df8b16ec8fb24b17097ba22e8e1bbf4b9148 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, cursor_parameter: 'str', limit_parameter: 'str | None', fixed_limit: 'int | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "cursor_feed_operation",
  "unit": "export"
}
```
