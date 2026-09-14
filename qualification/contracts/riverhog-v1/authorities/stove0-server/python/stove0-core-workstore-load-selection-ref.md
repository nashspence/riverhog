# stove0_core.WorkStore.load_selection_ref

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-load-selection-ref:8a3d4f7eb0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7ad1ed6a3d"></a>
- <a id="s-ae36b5c535"></a>`distribution`: `stove0-server`
- <a id="s-50b786f200"></a>`module`: `stove0_core`
- <a id="s-7e1c4e826b"></a>`name`: `load_selection_ref`
- <a id="s-edda386b47"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-c6f2eaaaf1"></a>`unit`: `member`

### Declared structure

- <a id="s-977fb69d8b"></a>`kind`: `"method"`
- <a id="s-6f29132a4f"></a>`signature`: `"\"(self, selection_sha256: 'str') -> 'ArtifactSelectionRef \| None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-0ab744150a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.load_selection_ref`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 76fb7f73c6da073026f7e0d45c7a04fddbdd7a1927b3f4cfeb216ed501b8099c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection_sha256: 'str') -> 'ArtifactSelectionRef | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_selection_ref",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```
