# stove0_target_support.TargetJobRequest.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetjobrequest-seal:314cde7722 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2954cb8be5"></a>
- <a id="s-32835d00d9"></a>`distribution`: `stove0-target-support`
- <a id="s-9fa8675d7c"></a>`module`: `stove0_target_support`
- <a id="s-09d6d20744"></a>`name`: `seal`
- <a id="s-1c5fd6ab19"></a>`owner`: `stove0_target_support.TargetJobRequest`
- <a id="s-6a2e7630d7"></a>`unit`: `member`

### Declared structure

- <a id="s-5017da4879"></a>`kind`: `"classmethod"`
- <a id="s-a25f0bd16c"></a>`signature`: `"\"(cls, declaration: 'TargetJobDeclaration', runtime: 'TargetRuntimeAuthority', callback_access: 'TargetCallbackAccess') -> 'TargetJobRequest'\""`

## Maintained corroboration

### Related interface records

- [TargetJobRequest](stove0-target-support-targetjobrequest.md)

## Governing policies

- <a id="pa-e96b2b31bf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetJobRequest.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 72a3be857c6ce40ffbcdfd34cc532a7d6eda2345558d5506e57993711e98dee7 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, declaration: 'TargetJobDeclaration', runtime: 'TargetRuntimeAuthority', callback_access: 'TargetCallbackAccess') -> 'TargetJobRequest'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "seal",
  "owner": "stove0_target_support.TargetJobRequest",
  "unit": "member"
}
```
