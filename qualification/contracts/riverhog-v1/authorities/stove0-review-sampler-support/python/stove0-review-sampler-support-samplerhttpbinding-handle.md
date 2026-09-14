# stove0_review_sampler_support.SamplerHttpBinding.handle

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-samplerhttp-8b928b8a4f:5055963742 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1ad5438dfa"></a>
- <a id="s-d43f5e2130"></a>`distribution`: `stove0-review-sampler-support`
- <a id="s-c89694c76b"></a>`module`: `stove0_review_sampler_support`
- <a id="s-ba7dd3ef70"></a>`name`: `handle`
- <a id="s-f6a43d13db"></a>`owner`: `stove0_review_sampler_support.SamplerHttpBinding`
- <a id="s-4e7617c03e"></a>`unit`: `member`

### Declared structure

- <a id="s-7a14fd0517"></a>`kind`: `"method"`
- <a id="s-3577eb4175"></a>`signature`: `"\"(self, method: 'str', path: 'str', body: 'bytes' = b'') -> 'SamplerHttpResponse'\""`

## Maintained corroboration

### Related interface records

- [SamplerHttpBinding](stove0-review-sampler-support-samplerhttpbinding.md)

## Governing policies

- <a id="pa-a3ce6efd78"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources.md#src-6dd798b0df) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.SamplerHttpBinding.handle`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 76f96c088469a4d26cd07af1b7f12f0b064fd0b3659ba44fa05600a9441e877d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, method: 'str', path: 'str', body: 'bytes' = b'') -> 'SamplerHttpResponse'\""
  },
  "distribution": "stove0-review-sampler-support",
  "module": "stove0_review_sampler_support",
  "name": "handle",
  "owner": "stove0_review_sampler_support.SamplerHttpBinding",
  "unit": "member"
}
```
