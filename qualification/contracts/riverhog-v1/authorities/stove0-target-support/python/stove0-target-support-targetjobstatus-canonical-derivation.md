# stove0_target_support.TargetJobStatus.canonical_derivation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetjobstatus-can-9dfced9919:a2ec1bde4b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2859a8919e"></a>
- <a id="s-76051858a8"></a>`distribution`: `stove0-target-support`
- <a id="s-cbe4f4dea8"></a>`module`: `stove0_target_support`
- <a id="s-173a9a7a8a"></a>`name`: `canonical_derivation`
- <a id="s-00ed773770"></a>`owner`: `stove0_target_support.TargetJobStatus`
- <a id="s-cf67d37f81"></a>`unit`: `member`

### Declared structure

- <a id="s-6afe679ec8"></a>`kind`: `"classmethod"`
- <a id="s-9c4731113f"></a>`signature`: `"\"(cls, value: 'dict[str, Any] \| None') -> 'dict[str, Any] \| None'\""`

## Maintained corroboration

### Related interface records

- [TargetJobStatus](stove0-target-support-targetjobstatus.md)

## Governing policies

- <a id="pa-f9b7ff5dfd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetJobStatus.canonical_derivation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7172fa1f774b6aad006f5b09067637dc1237d9e75f19dca60d261a3ff7cc8f10 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'dict[str, Any] | None') -> 'dict[str, Any] | None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "canonical_derivation",
  "owner": "stove0_target_support.TargetJobStatus",
  "unit": "member"
}
```
