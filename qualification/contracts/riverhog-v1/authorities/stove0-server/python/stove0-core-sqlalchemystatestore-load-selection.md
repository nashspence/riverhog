# stove0_core.SqlAlchemyStateStore.load_selection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-load-selection:343016ab06 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-536004c2d8"></a>
- <a id="s-1289bfe268"></a>`distribution`: `stove0-server`
- <a id="s-0295637d80"></a>`module`: `stove0_core`
- <a id="s-c294acf9ea"></a>`name`: `load_selection`
- <a id="s-6186122ec0"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-3dfe98ab21"></a>`unit`: `member`

### Declared structure

- <a id="s-d874452048"></a>`kind`: `"method"`
- <a id="s-e08e7cd702"></a>`signature`: `"\"(self, selection_sha256: 'str') -> 'ArtifactSelection \| None'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-edf9d3832e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.load_selection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 25b68eecceb72b215a13c005a7c666db0255b784335185308742a358ce789612 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection_sha256: 'str') -> 'ArtifactSelection | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_selection",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
