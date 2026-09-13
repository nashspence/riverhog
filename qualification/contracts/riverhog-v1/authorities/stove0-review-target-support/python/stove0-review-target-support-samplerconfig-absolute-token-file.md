# stove0_review_target_support.SamplerConfig.absolute_token_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-samplerconfi-bb345e938c:542303bd81 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-52e4379aa5"></a>
| Field | Shape |
|---|---|
| <a id="s-e02e0fe144"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-4dd6b153c0"></a>`distribution` | "stove0-review-target-support" |
| <a id="s-4384df315b"></a>`module` | "stove0_review_target_support" |
| <a id="s-8883366839"></a>`name` | "absolute_token_file" |
| <a id="s-6f5d4dd288"></a>`owner` | "stove0_review_target_support.SamplerConfig" |
| <a id="s-9ce6ea3588"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_review_target_support.SamplerConfig](stove0-review-target-support-samplerconfig.md)

## Governing policies

- <a id="pa-bccfdca26c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — `reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_support.SamplerConfig.absolute_token_file`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5091ea2b9805037930979a8d0e00da246f160fd6eb134c3c6ffe1165e6b98fdb -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'Path') -> 'Path'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "absolute_token_file",
  "owner": "stove0_review_target_support.SamplerConfig",
  "unit": "member"
}
```
