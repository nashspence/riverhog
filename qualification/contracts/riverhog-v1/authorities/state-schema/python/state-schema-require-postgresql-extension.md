# state_schema.require_postgresql_extension

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-require-postgresql-extension:8d2128507c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ba794c0229"></a>
| Field | Shape |
|---|---|
| <a id="s-c8fa5fc06e"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-c813d9e7aa"></a>`distribution` | "state-schema" |
| <a id="s-b55335f8ab"></a>`module` | "state_schema" |
| <a id="s-8b52125a03"></a>`name` | "require_postgresql_extension" |
| <a id="s-6c269ed802"></a>`unit` | "export" |

## Governing policies

- <a id="pa-3b514fe077"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.require_postgresql_extension`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e58edc643a6bd76d38b27c302df773322eb78df6035ffda7322c1236c7685daa -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(connection: 'Connection', *, name: 'str', schema: 'str', accepted_versions: 'tuple[str, ...]' = (), operator_classes: 'tuple[str, ...]' = ()) -> 'None'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "require_postgresql_extension",
  "unit": "export"
}
```
