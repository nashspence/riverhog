# http_api_contracts.http_operation_inventory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-http-operation-inventory:cee9831daf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-31da1d6f4b"></a>
| Field | Shape |
|---|---|
| <a id="s-6bd918df17"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ac35175b9c"></a>`distribution` | "http-api-contracts" |
| <a id="s-639a5286ed"></a>`module` | "http_api_contracts" |
| <a id="s-95f2244735"></a>`name` | "http_operation_inventory" |
| <a id="s-bcc473c510"></a>`unit` | "export" |

## Governing policies

- <a id="pa-a995cb6170"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.http_operation_inventory`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d26289885a630992ddee8def8fe7278365afa7100c9f5fa7221f69eb0b32a54f -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(contracts: 'Sequence[HttpOperationContract]') -> 'list[dict[str, Any]]'\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "http_operation_inventory",
  "unit": "export"
}
```
