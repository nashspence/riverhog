# riverhog_provenance_contracts.PROVENANCE_SCHEMA_DIALECT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-contracts:riverhog-provenance-contracts-provenance-7c32546bc2:5443192d75 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9aeb87c2d0"></a>
| Field | Shape |
|---|---|
| <a id="s-25dfd75967"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-fbf6f9e1e6"></a>`distribution` | "riverhog-provenance-contracts" |
| <a id="s-f98d867fd9"></a>`module` | "riverhog_provenance_contracts" |
| <a id="s-0c2d2a8601"></a>`name` | "PROVENANCE_SCHEMA_DIALECT" |
| <a id="s-47e6712d2c"></a>`unit` | "export" |

## Governing policies

- <a id="pa-9ea889f9b1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-contracts:riverhog_provenance_contracts](../../../evidence/sources.md#src-9b6289a988) — `packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance_contracts.PROVENANCE_SCHEMA_DIALECT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2ffec0210f68e5ca2549b43d409e95c2628523295a0f4a73a48d060e2ca54ac6 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "https://json-schema.org/draft/2020-12/schema"
  },
  "distribution": "riverhog-provenance-contracts",
  "module": "riverhog_provenance_contracts",
  "name": "PROVENANCE_SCHEMA_DIALECT",
  "unit": "export"
}
```
