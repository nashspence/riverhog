# state_schema.sqlite_engine

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-sqlite-engine:b8cfc1d486 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-582c2c6647"></a>
| Field | Shape |
|---|---|
| <a id="s-40eeb5c136"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-e4bfdcc35b"></a>`distribution` | "state-schema" |
| <a id="s-7c08b10604"></a>`module` | "state_schema" |
| <a id="s-d9e048abb0"></a>`name` | "sqlite_engine" |
| <a id="s-c07258d6b1"></a>`unit` | "export" |

## Governing policies

- <a id="pa-6d0d074c9f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.sqlite_engine`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8d215532131b4eb2dd11fecccd01855e76fddabf3d8721abda536c45ba4ccbe8 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(path: 'Path') -> 'Engine'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "sqlite_engine",
  "unit": "export"
}
```
