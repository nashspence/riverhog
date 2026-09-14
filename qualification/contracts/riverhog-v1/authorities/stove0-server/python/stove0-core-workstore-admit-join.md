# stove0_core.WorkStore.admit_join

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-admit-join:a25813edfd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eb76ae06a8"></a>
- <a id="s-0fa973bbfe"></a>`distribution`: `stove0-server`
- <a id="s-c16a1d5d90"></a>`module`: `stove0_core`
- <a id="s-264e22462b"></a>`name`: `admit_join`
- <a id="s-90e5285a34"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-3f0a30bfab"></a>`unit`: `member`

### Declared structure

- <a id="s-df33006263"></a>`kind`: `"method"`
- <a id="s-7b8a3e5fff"></a>`signature`: `"\"(self, work_id: 'str', *, expected_revision: 'int', plan: 'JoinPlan', selections: 'Sequence[ArtifactSelection]') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-c236295ffd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.admit_join`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e4480f4ba6c9de079354a49b6f60db8d5376dc89101a08ffe329ab5691dae1e7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, expected_revision: 'int', plan: 'JoinPlan', selections: 'Sequence[ArtifactSelection]') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "admit_join",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```
