# stove0_target_support.TransformPlan.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-transformplan-verify-digest:da9306755d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-427aa3bb57"></a>
- <a id="s-27b6c8ddbc"></a>`distribution`: `stove0-target-support`
- <a id="s-f28d1f81a3"></a>`module`: `stove0_target_support`
- <a id="s-63b0768a3e"></a>`name`: `verify_digest`
- <a id="s-76eb86745f"></a>`owner`: `stove0_target_support.TransformPlan`
- <a id="s-d3f082f8d8"></a>`unit`: `member`

### Declared structure

- <a id="s-3ac306c0e2"></a>`kind`: `"method"`
- <a id="s-4670e6e3e9"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [TransformPlan](stove0-target-support-transformplan.md)

## Governing policies

- <a id="pa-2bedb2b490"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TransformPlan.verify_digest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 717e9e5a95d2660dd35de051791c657120f439da7cc0dd2d8279f2de709f216a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "verify_digest",
  "owner": "stove0_target_support.TransformPlan",
  "unit": "member"
}
```
