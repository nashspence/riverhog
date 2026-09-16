# stove0_core.SqlAlchemyStateStore.prune_operational_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-prune-op-1fc57b49bf:1c1b3be83e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9510402234"></a>
- <a id="s-0b95810d8e"></a>`distribution`: `stove0-server`
- <a id="s-ebf26afdd5"></a>`module`: `stove0_core`
- <a id="s-9bc343a850"></a>`name`: `prune_operational_state`
- <a id="s-201abc3c28"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-166967cacf"></a>`unit`: `member`

### Declared structure

- <a id="s-59e51d40b6"></a>`kind`: `"method"`
- <a id="s-b7ff1806b5"></a>`signature`: `"\"(self, *, cutoff: 'str') -> 'dict[str, int]'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-fc8508edc6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.prune_operational_state`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dfc82d25009b64a7b7bdfa1405a29c3662b6fb4223877fd9f40586bdece03ab2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, cutoff: 'str') -> 'dict[str, int]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "prune_operational_state",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
