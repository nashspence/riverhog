# stove0_core.SqlAlchemyStateStore.load_target_production_seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-load-tar-f13034031b:b53d6612d8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-77565e396e"></a>
- <a id="s-b57f1f17f6"></a>`distribution`: `stove0-server`
- <a id="s-6c06305b7c"></a>`module`: `stove0_core`
- <a id="s-1acd8bd4b1"></a>`name`: `load_target_production_seal`
- <a id="s-ef4eb2e23e"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-04a63c2d54"></a>`unit`: `member`

### Declared structure

- <a id="s-e79b6a28b8"></a>`kind`: `"method"`
- <a id="s-5d624eb01a"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord \| None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-26bb6dd9a1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.load_target_production_seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 04e0a7bbb10f0a64102abfe974b03b4d4be507698375ff23a9954903077dd0c3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_target_production_seal",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
