# stove0_core.SqlAlchemyStateStore.iter_target_outputs_by_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-iter-tar-459f2a5134:8c9ee57d3a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9bfd0b632e"></a>
| Field | Shape |
|---|---|
| <a id="s-69b5f392d2"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-9b0bf7b75c"></a>`distribution` | "stove0-server" |
| <a id="s-5ab4e61846"></a>`module` | "stove0_core" |
| <a id="s-7d3bd58c1c"></a>`name` | "iter_target_outputs_by_path" |
| <a id="s-ea9de11da4"></a>`owner` | "stove0_core.SqlAlchemyStateStore" |
| <a id="s-f5c0438254"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-7d0cdaecd7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.iter_target_outputs_by_path`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 92d68f30aecfa0b73b76e74fd339c55f192b0370fce635e44e2a8ec1566e355e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_target_outputs_by_path",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
