# stove0_core.SqlAlchemyStateStore.admit_join

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-admit-join:74d2f99333 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-74bd9a19bc"></a>
- <a id="s-bafa5cd536"></a>`distribution`: `stove0-server`
- <a id="s-d62719738c"></a>`module`: `stove0_core`
- <a id="s-3a20d923ad"></a>`name`: `admit_join`
- <a id="s-0a147262cf"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-79c36d89e2"></a>`unit`: `member`

### Declared structure

- <a id="s-5f6b3b20b1"></a>`kind`: `"method"`
- <a id="s-6786b3639d"></a>`signature`: `"\"(self, work_id: 'str', *, expected_revision: 'int', plan: 'JoinPlan', selections: 'Sequence[ArtifactSelection]') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-9d9c45b96d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.admit_join`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d5f7666d29d925140bea524f3163803788fed5e4cecdbd0137eb257575d33bca -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, expected_revision: 'int', plan: 'JoinPlan', selections: 'Sequence[ArtifactSelection]') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "admit_join",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
