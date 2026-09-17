# stove0_core.SqlAlchemyStateStore.list_events

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-list-events:480ee16ec5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ba9937df60"></a>
- <a id="s-4fa296f3d0"></a>`distribution`: `stove0-server`
- <a id="s-e8dbdffdcc"></a>`module`: `stove0_core`
- <a id="s-b6d16d432b"></a>`name`: `list_events`
- <a id="s-1995b26ace"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-d38d09bd46"></a>`unit`: `member`

### Declared structure

- <a id="s-7d93355a5e"></a>`kind`: `"method"`
- <a id="s-bd6225a82b"></a>`signature`: `"\"(self, *, after: 'str \| None' = None, limit: 'int' = 100) -> 'Stove0EventPage'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-7db5e1b430"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.list_events`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 57d322de21f6b57f8fe4895e5c918617d274ba9b2902ce7bfa2c5b6e2a334680 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, after: 'str | None' = None, limit: 'int' = 100) -> 'Stove0EventPage'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "list_events",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
