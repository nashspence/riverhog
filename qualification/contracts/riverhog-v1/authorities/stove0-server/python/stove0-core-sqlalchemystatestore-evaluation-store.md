# stove0_core.SqlAlchemyStateStore.evaluation_store

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-evaluation-store:030c87f865 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c616276187"></a>
- <a id="s-6207d38502"></a>`distribution`: `stove0-server`
- <a id="s-7b9e052363"></a>`module`: `stove0_core`
- <a id="s-57eae8dfa0"></a>`name`: `evaluation_store`
- <a id="s-8a26f75ba8"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-b58a290441"></a>`unit`: `member`

### Declared structure

- <a id="s-6741565fa3"></a>`kind`: `"method"`
- <a id="s-ea61467e15"></a>`signature`: `"\"(self) -> '_EvaluationStoreView'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-a68b425e55"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.evaluation_store`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 973481a38d9a5259176ec44fa565277e72b4880a71a19b174eb37f7e97093139 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> '_EvaluationStoreView'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "evaluation_store",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
