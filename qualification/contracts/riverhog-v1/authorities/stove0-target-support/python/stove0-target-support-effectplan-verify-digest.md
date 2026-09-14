# stove0_target_support.EffectPlan.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-effectplan-verify-digest:7d5dd46b37 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1e3d0e3b0a"></a>
- <a id="s-3230abaeb2"></a>`distribution`: `stove0-target-support`
- <a id="s-77bbe961dd"></a>`module`: `stove0_target_support`
- <a id="s-f8db65d753"></a>`name`: `verify_digest`
- <a id="s-6fabec8e0d"></a>`owner`: `stove0_target_support.EffectPlan`
- <a id="s-ca2ee586d3"></a>`unit`: `member`

### Declared structure

- <a id="s-6c49c77bae"></a>`kind`: `"method"`
- <a id="s-383bb4ef60"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_support.EffectPlan](stove0-target-support-effectplan.md)

## Governing policies

- <a id="pa-88176221a1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.EffectPlan.verify_digest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 44993354cc1daf04408ee3a7fd583aa8901f97ceac3ef990505d8faecca5c655 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "verify_digest",
  "owner": "stove0_target_support.EffectPlan",
  "unit": "member"
}
```
