# stove0_target_support.TargetCollectionPublication.finish_success

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetcollectionpub-a56ea7da34:deb533f8fc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-22b5e5b3fe"></a>
- <a id="s-840dc8210a"></a>`distribution`: `stove0-target-support`
- <a id="s-bcdb581dc5"></a>`module`: `stove0_target_support`
- <a id="s-b62c9e7925"></a>`name`: `finish_success`
- <a id="s-2a03f6c38f"></a>`owner`: `stove0_target_support.TargetCollectionPublication`
- <a id="s-1c8a8035db"></a>`unit`: `member`

### Declared structure

- <a id="s-88e9c64fc1"></a>`kind`: `"method"`
- <a id="s-2d85c5cb06"></a>`signature`: `"\"(self, *, operation: 'OperationContract', execution_sha256: 'str', attempt: 'int' = 1, runtime_evidence: 'Mapping[str, object] \| None' = None, **kwargs: 'Any') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [TargetCollectionPublication](stove0-target-support-targetcollectionpublication.md)

## Governing policies

- <a id="pa-2a87092652"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetCollectionPublication.finish_success`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f43b30fb76d8429c379f3199045c457845455ea0172cafce54c7cec529f52f5b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, operation: 'OperationContract', execution_sha256: 'str', attempt: 'int' = 1, runtime_evidence: 'Mapping[str, object] | None' = None, **kwargs: 'Any') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "finish_success",
  "owner": "stove0_target_support.TargetCollectionPublication",
  "unit": "member"
}
```

</details>
