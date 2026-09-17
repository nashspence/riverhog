# stove0_core.InMemoryWorkStore.compare_and_swap_target_production_seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-compare-and-9d018a8cab:2423286a54 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-216f5c27af"></a>
- <a id="s-9791e21c12"></a>`distribution`: `stove0-server`
- <a id="s-523b57c348"></a>`module`: `stove0_core`
- <a id="s-3e9c834eab"></a>`name`: `compare_and_swap_target_production_seal`
- <a id="s-8eeda53f32"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-61056b17bc"></a>`unit`: `member`

### Declared structure

- <a id="s-feca51918c"></a>`kind`: `"method"`
- <a id="s-abcda2973d"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', *, expected_revision: 'int', replacement: 'TargetProductionSealRecord') -> 'TargetProductionSealRecord'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-c2ca52fbb8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.compare_and_swap_target_production_seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aa9f680ec96e4a55dcd30d9a52cec272ef27e0a49c949dd9495316d8642d6c85 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', *, expected_revision: 'int', replacement: 'TargetProductionSealRecord') -> 'TargetProductionSealRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "compare_and_swap_target_production_seal",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
