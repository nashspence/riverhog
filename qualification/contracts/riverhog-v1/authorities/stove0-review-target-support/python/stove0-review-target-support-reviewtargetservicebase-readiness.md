# stove0_review_target_support.ReviewTargetServiceBase.readiness

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-reviewtarget-a21338417b:b8a675a264 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-09fb760186"></a>
- <a id="s-9c6a54c3d0"></a>`distribution`: `stove0-review-target-support`
- <a id="s-c933aaf325"></a>`module`: `stove0_review_target_support`
- <a id="s-e4215d3895"></a>`name`: `readiness`
- <a id="s-17bfae8e0a"></a>`owner`: `stove0_review_target_support.ReviewTargetServiceBase`
- <a id="s-7a0d077a60"></a>`unit`: `member`

### Declared structure

- <a id="s-a452f2cf03"></a>`kind`: `"method"`
- <a id="s-52ff9fd907"></a>`signature`: `"\"(self) -> 'dict[str, str]'\""`

## Maintained corroboration

### Related interface records

- [ReviewTargetServiceBase](stove0-review-target-support-reviewtargetservicebase.md)

## Governing policies

- <a id="pa-8bb5ce0832"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — `reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_support.ReviewTargetServiceBase.readiness`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 68596188ae3839e29f03f9fcf1d27780b5676ae3698c7cdfd7431570b078d013 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, str]'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "readiness",
  "owner": "stove0_review_target_support.ReviewTargetServiceBase",
  "unit": "member"
}
```
