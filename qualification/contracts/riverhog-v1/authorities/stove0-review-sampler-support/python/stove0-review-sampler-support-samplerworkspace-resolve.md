# stove0_review_sampler_support.SamplerWorkspace.resolve

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-samplerwork-071d51fcdd:53d77d9ed3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d58fbb63d2"></a>
- <a id="s-3345297be0"></a>`distribution`: `stove0-review-sampler-support`
- <a id="s-c8a30d7832"></a>`module`: `stove0_review_sampler_support`
- <a id="s-3d6de315f2"></a>`name`: `resolve`
- <a id="s-72503ceee8"></a>`owner`: `stove0_review_sampler_support.SamplerWorkspace`
- <a id="s-8f4a70733e"></a>`unit`: `member`

### Declared structure

- <a id="s-3b01302ffc"></a>`kind`: `"method"`
- <a id="s-27af3432a3"></a>`signature`: `"\"(self, relative_path: 'str') -> 'Path'\""`

## Maintained corroboration

### Related interface records

- [SamplerWorkspace](stove0-review-sampler-support-samplerworkspace.md)

## Governing policies

- <a id="pa-3879f086eb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources.md#src-6dd798b0df) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.SamplerWorkspace.resolve`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74ee25ac5bb0febc92be1e24145e19d00aa839c3745ad15a5ea06afa34aa6f0e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, relative_path: 'str') -> 'Path'\""
  },
  "distribution": "stove0-review-sampler-support",
  "module": "stove0_review_sampler_support",
  "name": "resolve",
  "owner": "stove0_review_sampler_support.SamplerWorkspace",
  "unit": "member"
}
```
