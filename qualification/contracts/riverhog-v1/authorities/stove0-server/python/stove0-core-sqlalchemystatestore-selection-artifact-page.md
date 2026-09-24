# stove0_core.SqlAlchemyStateStore.selection_artifact_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-selectio-5048662db1:62e82d605c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-41b8cd2a8b"></a>
- <a id="s-fa60f17b37"></a>`distribution`: `stove0-server`
- <a id="s-ea5c65a788"></a>`module`: `stove0_core`
- <a id="s-16127557ba"></a>`name`: `selection_artifact_page`
- <a id="s-8bf8565224"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-b0a3a9a497"></a>`unit`: `member`

### Declared structure

- <a id="s-240c9fdc5f"></a>`kind`: `"method"`
- <a id="s-e0b21ef147"></a>`signature`: `"\"(self, selection_sha256: 'str', *, continuation: 'str \| None', limit: 'int') -> 'tuple[tuple[WorkArtifactSubject, ...], str \| None, bool]'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-4b65295e67"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.selection_artifact_page`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b476126247d6c55e060a2941cd3820fcb35160d7f311fb09087fbf87051d0ba1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection_sha256: 'str', *, continuation: 'str | None', limit: 'int') -> 'tuple[tuple[WorkArtifactSubject, ...], str | None, bool]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "selection_artifact_page",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
