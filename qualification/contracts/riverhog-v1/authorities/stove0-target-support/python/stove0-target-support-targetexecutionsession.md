# stove0_target_support.TargetExecutionSession

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutionsession:bbe8733f6b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e319faa460"></a>
- <a id="s-d9af6d7ea0"></a>`distribution`: `stove0-target-support`
- <a id="s-7f99cfabd7"></a>`module`: `stove0_target_support`
- <a id="s-721e18b062"></a>`name`: `TargetExecutionSession`
- <a id="s-04844de2b8"></a>`unit`: `export`

### Declared structure

- <a id="s-fa18826ad1"></a>`kind`: `"class"`
- <a id="s-991e33af37"></a>`signature`: `"\"(request: 'TargetJobRequest', attempt: 'int', runtime_registry: 'ClaimedCollectionRuntimeRegistry') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [completed_status](stove0-target-support-targetexecutionsession-completed-status.md)
- [record_completed](stove0-target-support-targetexecutionsession-record-completed.md)

## Governing policies

- <a id="pa-05ec76b3f8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionSession`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0b8894106f7d1c918b86c3251133d2ada1f7baf80acb0d6865508acf1798f13b -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(request: 'TargetJobRequest', attempt: 'int', runtime_registry: 'ClaimedCollectionRuntimeRegistry') -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetExecutionSession",
  "unit": "export"
}
```

</details>
