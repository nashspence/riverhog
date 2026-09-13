# stove0_core.SqlAlchemyStateStore.load_selection_artifact

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-load-sel-c28ec866f6:3ef7442d27 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9ac807eb0c"></a>
| Field | Shape |
|---|---|
| <a id="s-f1c5a2809b"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-1d2058bac2"></a>`distribution` | "stove0-server" |
| <a id="s-8fdd7c7c72"></a>`module` | "stove0_core" |
| <a id="s-aaefd02768"></a>`name` | "load_selection_artifact" |
| <a id="s-c9d7d9ed3e"></a>`owner` | "stove0_core.SqlAlchemyStateStore" |
| <a id="s-6e8245cd16"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-d89122ace3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.load_selection_artifact`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f4310c1acb72cea8d0dd219ce3e85f480215ee2df385f2f1fe16d6ddaee4b526 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection_sha256: 'str', artifact_id: 'str') -> 'ArtifactSubject | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_selection_artifact",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
