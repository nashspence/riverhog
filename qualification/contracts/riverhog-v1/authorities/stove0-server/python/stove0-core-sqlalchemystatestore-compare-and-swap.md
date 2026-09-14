# stove0_core.SqlAlchemyStateStore.compare_and_swap

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-compare-and-swap:5447278f1e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bce11a33d9"></a>
- <a id="s-707913b1c9"></a>`distribution`: `stove0-server`
- <a id="s-5567470758"></a>`module`: `stove0_core`
- <a id="s-e6583b7825"></a>`name`: `compare_and_swap`
- <a id="s-58eef69cfb"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-78664fbc58"></a>`unit`: `member`

### Declared structure

- <a id="s-348647bcec"></a>`kind`: `"method"`
- <a id="s-960bf480b1"></a>`signature`: `"\"(self, work_id: 'str', *, expected_revision: 'int', replacement: 'WorkRecord') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-e0a2a9cc04"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.compare_and_swap`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e1ff3df76807689b8a98029465529ccd108588bed51c05c56034a72b97e86f2c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, expected_revision: 'int', replacement: 'WorkRecord') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "compare_and_swap",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
