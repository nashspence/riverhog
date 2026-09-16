# stove0_core.SqlAlchemyStateStore.create

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-create:ceabd0e32a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fe5a9dbdf3"></a>
- <a id="s-1c1d39e944"></a>`distribution`: `stove0-server`
- <a id="s-d8e481a2eb"></a>`module`: `stove0_core`
- <a id="s-8b65bfe552"></a>`name`: `create`
- <a id="s-68d92b25af"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-0a1c6b3441"></a>`unit`: `member`

### Declared structure

- <a id="s-676c20711d"></a>`kind`: `"method"`
- <a id="s-39efffc6ad"></a>`signature`: `"\"(self, record: 'WorkRecord') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-eb8cfd0161"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.create`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6145f142625c4fa5321b05fccdd5aaee06387a6f4c9116e6bf900b7921c3da62 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "create",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
