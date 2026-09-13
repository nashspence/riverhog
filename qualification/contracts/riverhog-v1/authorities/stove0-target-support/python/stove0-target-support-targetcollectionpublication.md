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
| Field | Shape |
|---|---|
| <a id="s-ba21178d28"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-848c605abb"></a>`distribution` | "stove0-target-support" |
| <a id="s-72ae20d8c8"></a>`module` | "stove0_target_support" |
| <a id="s-6993df32d1"></a>`name` | "TargetCollectionPublication" |
| <a id="s-2564e236fe"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetCollectionPublication.append](stove0-target-support-targetcollectionpublication-append.md)
- [stove0_target_support.TargetCollectionPublication.finish_success](stove0-target-support-targetcollectionpublication-finish-success.md)

## Governing policies

- <a id="pa-6eae21e6d1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetCollectionPublication`

### Exact owned JSON

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
