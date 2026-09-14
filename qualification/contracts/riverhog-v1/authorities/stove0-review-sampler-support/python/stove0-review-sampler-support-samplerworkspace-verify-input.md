# stove0_review_sampler_support.SamplerWorkspace.verify_input

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-samplerwork-b116471981:5d0978c79f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9c186e5e70"></a>
- <a id="s-6f5916fbd7"></a>`distribution`: `stove0-review-sampler-support`
- <a id="s-a49e263d23"></a>`module`: `stove0_review_sampler_support`
- <a id="s-c9d03fbe4e"></a>`name`: `verify_input`
- <a id="s-28ff26f491"></a>`owner`: `stove0_review_sampler_support.SamplerWorkspace`
- <a id="s-8ff0fe6ff4"></a>`unit`: `member`

### Declared structure

- <a id="s-bebc37de5e"></a>`kind`: `"method"`
- <a id="s-94f141cb15"></a>`signature`: `"\"(self, declared: 'SamplerInput') -> 'Path'\""`

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_support.SamplerWorkspace](stove0-review-sampler-support-samplerworkspace.md)

## Governing policies

- <a id="pa-13b94eda02"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources.md#src-6dd798b0df) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.SamplerWorkspace.verify_input`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b8d9bf1269b9f4aa91a601b94ed6ed7f2dc4321773a72ffa8f3a8f1640cb160b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, declared: 'SamplerInput') -> 'Path'\""
  },
  "distribution": "stove0-review-sampler-support",
  "module": "stove0_review_sampler_support",
  "name": "verify_input",
  "owner": "stove0_review_sampler_support.SamplerWorkspace",
  "unit": "member"
}
```
