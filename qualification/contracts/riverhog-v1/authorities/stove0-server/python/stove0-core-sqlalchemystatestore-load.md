# stove0_core.SqlAlchemyStateStore.load

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-load:ceb35b7e13 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cce043d571"></a>
- <a id="s-ded01bf898"></a>`distribution`: `stove0-server`
- <a id="s-f7645e4bb8"></a>`module`: `stove0_core`
- <a id="s-bd2113760b"></a>`name`: `load`
- <a id="s-b6ae01318a"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-1de689661e"></a>`unit`: `member`

### Declared structure

- <a id="s-ed5e53cecc"></a>`kind`: `"method"`
- <a id="s-1d65d6bb31"></a>`signature`: `"\"(self, work_id: 'str') -> 'WorkRecord \| None'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-2305bce0a6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.load`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2c1fc6d08b839620a3f07563660c11d240f91eb041c4b4439306e6ba291216eb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str') -> 'WorkRecord | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
