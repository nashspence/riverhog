# stove0_core.SqlAlchemyStateStore.load_selection_ref

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-load-selection-ref:bd7469f6e2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-988dab9a51"></a>
- <a id="s-16593b8d9b"></a>`distribution`: `stove0-server`
- <a id="s-7ff9279e40"></a>`module`: `stove0_core`
- <a id="s-59ed672cb3"></a>`name`: `load_selection_ref`
- <a id="s-2dcd6898aa"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-dea62adce6"></a>`unit`: `member`

### Declared structure

- <a id="s-b37acd5005"></a>`kind`: `"method"`
- <a id="s-3901dc0cd0"></a>`signature`: `"\"(self, selection_sha256: 'str') -> 'ArtifactSelectionRef \| None'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-ac70982eea"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.load_selection_ref`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a66fffef00739e2d219d1eff86d156fbcd86b9a231c84de3cf2dbbe62a798330 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection_sha256: 'str') -> 'ArtifactSelectionRef | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_selection_ref",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
