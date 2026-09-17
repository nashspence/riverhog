# stove0_core.InMemoryWorkStore.load_selection_ref

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-load-selection-ref:7fd2e677c5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fea34d4833"></a>
- <a id="s-308c1ce44b"></a>`distribution`: `stove0-server`
- <a id="s-4c62d639cc"></a>`module`: `stove0_core`
- <a id="s-08568817a2"></a>`name`: `load_selection_ref`
- <a id="s-3beae188a8"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-714ae1e031"></a>`unit`: `member`

### Declared structure

- <a id="s-804dfebffd"></a>`kind`: `"method"`
- <a id="s-02643108c4"></a>`signature`: `"\"(self, selection_sha256: 'str') -> 'ArtifactSelectionRef \| None'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-bf21ef3140"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.load_selection_ref`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1becf4a05c685b3295d1b45d5149e1bb383b2bcb32df0d510ef6da3cd689ef35 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection_sha256: 'str') -> 'ArtifactSelectionRef | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_selection_ref",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
