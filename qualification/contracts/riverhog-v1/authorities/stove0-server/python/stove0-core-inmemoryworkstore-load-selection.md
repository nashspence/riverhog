# stove0_core.InMemoryWorkStore.load_selection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-load-selection:4081801b98 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1b9b24e852"></a>
- <a id="s-1f91ca4d28"></a>`distribution`: `stove0-server`
- <a id="s-77a4cd63d3"></a>`module`: `stove0_core`
- <a id="s-e6dfd217c4"></a>`name`: `load_selection`
- <a id="s-a489865124"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-868f850f9c"></a>`unit`: `member`

### Declared structure

- <a id="s-d726c9c2dc"></a>`kind`: `"method"`
- <a id="s-5f45484120"></a>`signature`: `"\"(self, selection_sha256: 'str') -> 'ArtifactSelection \| None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-2637650767"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.load_selection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5a5864d7caa32bdf4be3840dc1ad28e72b27c27c9241d74f5abf3368a0657d11 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection_sha256: 'str') -> 'ArtifactSelection | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_selection",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```
