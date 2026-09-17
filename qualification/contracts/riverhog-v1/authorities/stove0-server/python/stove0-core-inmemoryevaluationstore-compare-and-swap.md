# stove0_core.InMemoryEvaluationStore.compare_and_swap

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryevaluationstore-compare-and-swap:f325597173 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-63e701c22d"></a>
- <a id="s-2b083ee99d"></a>`distribution`: `stove0-server`
- <a id="s-aa2e9a14b0"></a>`module`: `stove0_core`
- <a id="s-35d4824c1f"></a>`name`: `compare_and_swap`
- <a id="s-f18d52ef5e"></a>`owner`: `stove0_core.InMemoryEvaluationStore`
- <a id="s-cd98939d28"></a>`unit`: `member`

### Declared structure

- <a id="s-c609ff02cc"></a>`kind`: `"method"`
- <a id="s-3d412106e3"></a>`signature`: `"\"(self, evaluation_id: 'str', *, expected_revision: 'int', replacement: 'EvaluationRecord') -> 'EvaluationRecord'\""`

## Maintained corroboration

### Related interface records

- [InMemoryEvaluationStore](stove0-core-inmemoryevaluationstore.md)

## Governing policies

- <a id="pa-c111ccb842"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.InMemoryEvaluationStore.compare_and_swap`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 877f04e9a5b0db82d6149c9a102d3f184c9e89fdaac765b58a6bc78010cd5409 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, evaluation_id: 'str', *, expected_revision: 'int', replacement: 'EvaluationRecord') -> 'EvaluationRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "compare_and_swap",
  "owner": "stove0_core.InMemoryEvaluationStore",
  "unit": "member"
}
```

</details>
