# stove0_review_sampler_support.SamplerWorkspace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-samplerworkspace:b75730e41a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-923788cf05"></a>
| Field | Shape |
|---|---|
| <a id="s-a2c7ea4439"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-9615ebf1a8"></a>`distribution` | "stove0-review-sampler-support" |
| <a id="s-73b9c9217f"></a>`module` | "stove0_review_sampler_support" |
| <a id="s-0fa645a5ba"></a>`name` | "SamplerWorkspace" |
| <a id="s-d52cc8c538"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_support.SamplerWorkspace.resolve](stove0-review-sampler-support-samplerworkspace-resolve.md)
- [stove0_review_sampler_support.SamplerWorkspace.canceled](stove0-review-sampler-support-samplerworkspace-canceled.md)
- [stove0_review_sampler_support.SamplerWorkspace.verify_input](stove0-review-sampler-support-samplerworkspace-verify-input.md)
- [stove0_review_sampler_support.SamplerWorkspace.output](stove0-review-sampler-support-samplerworkspace-output.md)

## Governing policies

- <a id="pa-df96852f0c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources.md#src-6dd798b0df) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.SamplerWorkspace`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c00c548749ba0cf6f01ab74cae725d8b6f7645756d4ed0641292031bd0a7bffe -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(root: 'Path', request: 'SamplerRequest') -> 'None'\""
  },
  "distribution": "stove0-review-sampler-support",
  "module": "stove0_review_sampler_support",
  "name": "SamplerWorkspace",
  "unit": "export"
}
```
