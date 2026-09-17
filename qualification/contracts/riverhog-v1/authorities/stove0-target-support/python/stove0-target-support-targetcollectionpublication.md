# stove0_target_support.TargetCollectionPublication

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetcollectionpublication:4ee199fe3d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-789d2e92bb"></a>
- <a id="s-848c605abb"></a>`distribution`: `stove0-target-support`
- <a id="s-72ae20d8c8"></a>`module`: `stove0_target_support`
- <a id="s-6993df32d1"></a>`name`: `TargetCollectionPublication`
- <a id="s-2564e236fe"></a>`unit`: `export`

### Declared structure

- <a id="s-af84fd7d9d"></a>`kind`: `"class"`
- <a id="s-0e9f7663d9"></a>`signature`: `"\"(execution: 'TargetExecutionRuntime', writer: 'IncrementalDerivedCollectionWriter') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [append](stove0-target-support-targetcollectionpublication-append.md)
- [finish_success](stove0-target-support-targetcollectionpublication-finish-success.md)

## Governing policies

- <a id="pa-6eae21e6d1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetCollectionPublication`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 67a47a6212db502a30e224564edae68c8312a0465f296c6bdaf6d8473943f397 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(execution: 'TargetExecutionRuntime', writer: 'IncrementalDerivedCollectionWriter') -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetCollectionPublication",
  "unit": "export"
}
```

</details>
