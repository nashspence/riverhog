# stove0_review_sampler_support.sampler_schema_bundle

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support-sampler-schema-bundle:bc6cdf2ba7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f7df926893"></a>
| Field | Shape |
|---|---|
| <a id="s-8a3e9947b1"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-b3274fc18c"></a>`distribution` | "stove0-review-sampler-support" |
| <a id="s-3c3b0fb130"></a>`module` | "stove0_review_sampler_support" |
| <a id="s-8bcb0939d7"></a>`name` | "sampler_schema_bundle" |
| <a id="s-67a1ff36ce"></a>`unit` | "export" |

## Governing policies

- <a id="pa-21a5e93a82"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources.md#src-6dd798b0df) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_support.sampler_schema_bundle`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0fbec799b2dacac7cc35dd0fc8ea27307a7f90a075eac14b02faf53511e7fa74 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'dict[str, Any]'\""
  },
  "distribution": "stove0-review-sampler-support",
  "module": "stove0_review_sampler_support",
  "name": "sampler_schema_bundle",
  "unit": "export"
}
```
