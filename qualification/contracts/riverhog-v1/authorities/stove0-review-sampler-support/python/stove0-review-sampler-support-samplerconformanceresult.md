# stove0_review_sampler_support.SamplerConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-samplerconf-15c2d6a093:ee479321c1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9db013f7de"></a>
| Field | Shape |
|---|---|
| <a id="s-083ddb02d8"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-44a0d7e79b"></a>`distribution` | "stove0-review-sampler-support" |
| <a id="s-64c3edbcd1"></a>`module` | "stove0_review_sampler_support" |
| <a id="s-468fbe5cc1"></a>`name` | "SamplerConformanceResult" |
| <a id="s-6867d7b6b8"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_support.SamplerConformanceResult.validate_result](stove0-review-sampler-support-samplerconformanceresult-validate-result.md)

## Governing policies

- <a id="pa-819d258632"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources.md#src-6dd798b0df) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.SamplerConformanceResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c169329cfe059bdf503b951bcf1fcac61163296aeb0630c2715606c182487c02 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "04925f6f55b8bdb122d1cfdcba5aa12da892ebae3bdc0f0f0f6955364773c80f",
    "signature": "\"(*, format: Literal['stove0-review-sampler-conformance-result/v1'] = 'stove0-review-sampler-conformance-result/v1', status: Literal['conformant', 'inspected'], sampler: stove0_review_sampler_protocol.SamplerDescriptor, coverage: stove0_review_sampler_support.conformance.SamplerConformanceCoverage, sampling: Literal['exercised', 'not-exercised'], request: stove0_review_sampler_protocol.SamplerRequest | None = None, sample: stove0_review_sampler_protocol.SamplerResult | None = None) -> None\""
  },
  "distribution": "stove0-review-sampler-support",
  "module": "stove0_review_sampler_support",
  "name": "SamplerConformanceResult",
  "unit": "export"
}
```
