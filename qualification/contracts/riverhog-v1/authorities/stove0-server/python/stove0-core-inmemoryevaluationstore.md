# stove0_core.InMemoryEvaluationStore

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryevaluationstore:3afa6dd972 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-95c0bbcfe8"></a>
- <a id="s-73bfb2a538"></a>`distribution`: `stove0-server`
- <a id="s-43d6124514"></a>`module`: `stove0_core`
- <a id="s-cc721f97e6"></a>`name`: `InMemoryEvaluationStore`
- <a id="s-a285dc43bf"></a>`unit`: `export`

### Declared structure

- <a id="s-55c5d9d2da"></a>`kind`: `"class"`
- <a id="s-3a64334cd9"></a>`signature`: `"\"() -> 'None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.InMemoryEvaluationStore.compare_and_swap](stove0-core-inmemoryevaluationstore-compare-and-swap.md)
- [stove0_core.InMemoryEvaluationStore.create](stove0-core-inmemoryevaluationstore-create.md)
- [stove0_core.InMemoryEvaluationStore.load](stove0-core-inmemoryevaluationstore-load.md)

## Governing policies

- <a id="pa-e628cc0c2b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryEvaluationStore`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0a9231da996bf2eb45b03a1f81b81277b5a7897e743b11886638b8be482e2244 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"() -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "InMemoryEvaluationStore",
  "unit": "export"
}
```
