# stove0_core.SqlAlchemyStateStore.compare_and_swap_cursor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-compare-c09f8a3b6b:2b685030d4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-513c00f014"></a>
- <a id="s-e6f00d3ebf"></a>`distribution`: `stove0-server`
- <a id="s-6392ff6f48"></a>`module`: `stove0_core`
- <a id="s-c0489e9d11"></a>`name`: `compare_and_swap_cursor`
- <a id="s-9c9d042c1d"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-b2cf569788"></a>`unit`: `member`

### Declared structure

- <a id="s-9e31347029"></a>`kind`: `"method"`
- <a id="s-4e0a4c003a"></a>`signature`: `"\"(self, stream: 'str', *, expected_revision: 'int \| None', cursor: 'str') -> 'tuple[str, int]'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-08a94a0831"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.compare_and_swap_cursor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dcd2892908ffbba1edd4d7b0dd6fd3cf089734466abbd9ee9fa98e4283374c15 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, stream: 'str', *, expected_revision: 'int | None', cursor: 'str') -> 'tuple[str, int]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "compare_and_swap_cursor",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
