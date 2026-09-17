# stove0_core.SqlAlchemyStateStore.load_cursor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-load-cursor:8b00d82347 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2e2883be1c"></a>
- <a id="s-b8d7442636"></a>`distribution`: `stove0-server`
- <a id="s-95cbc408c4"></a>`module`: `stove0_core`
- <a id="s-7253949755"></a>`name`: `load_cursor`
- <a id="s-17e11096e7"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-9816e25425"></a>`unit`: `member`

### Declared structure

- <a id="s-dd10afa8a2"></a>`kind`: `"method"`
- <a id="s-687d702ca3"></a>`signature`: `"\"(self, stream: 'str') -> 'tuple[str, int] \| None'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-25e01d2262"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.load_cursor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c885c87a3d07e541111f531defa567067abc476d426787e1efbe8d6ecce3033b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, stream: 'str') -> 'tuple[str, int] | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_cursor",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
