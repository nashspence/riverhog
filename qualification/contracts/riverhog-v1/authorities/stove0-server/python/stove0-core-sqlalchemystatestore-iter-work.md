# stove0_core.SqlAlchemyStateStore.iter_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-iter-work:6d0c90fc38 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f0c9000c9d"></a>
- <a id="s-3a3168aa43"></a>`distribution`: `stove0-server`
- <a id="s-f773addb55"></a>`module`: `stove0_core`
- <a id="s-675a44b3b3"></a>`name`: `iter_work`
- <a id="s-08792295d4"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-ca2a326c9a"></a>`unit`: `member`

### Declared structure

- <a id="s-24cf580f5a"></a>`kind`: `"method"`
- <a id="s-8046dd4cde"></a>`signature`: `"\"(self, *, phase: 'str \| None' = None, query: 'str \| None' = None, sort: 'WorkSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'Iterator[WorkRecord]'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-4c6d1162ce"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.iter_work`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cbcc19db9c05737e37fa80e61c6ac20c45065333b4e751af0242cb2c0d04f405 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, phase: 'str | None' = None, query: 'str | None' = None, sort: 'WorkSort' = 'updated_at', order: 'SortOrder' = 'desc') -> 'Iterator[WorkRecord]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_work",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
