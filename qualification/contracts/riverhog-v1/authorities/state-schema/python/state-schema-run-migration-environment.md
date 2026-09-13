# state_schema.run_migration_environment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-run-migration-environment:06e4538d02 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fa7bc44bf8"></a>
| Field | Shape |
|---|---|
| <a id="s-4f13460f0c"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-9ef7de4f77"></a>`distribution` | "state-schema" |
| <a id="s-ab4d016280"></a>`module` | "state_schema" |
| <a id="s-ce5db6d568"></a>`name` | "run_migration_environment" |
| <a id="s-9048fcbacc"></a>`unit` | "export" |

## Governing policies

- <a id="pa-546145a793"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.run_migration_environment`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 57d6ea13eecc33fb1c0d4ffe4d70e1157df3af695c2d42e4c426264fce57866d -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'None'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "run_migration_environment",
  "unit": "export"
}
```
