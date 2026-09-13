# stove0_review_target_support.ReviewTargetConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-reviewtargetconfig:de19460673 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e55fe2bc05"></a>
| Field | Shape |
|---|---|
| <a id="s-492eb21fa4"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-e72f049f29"></a>`distribution` | "stove0-review-target-support" |
| <a id="s-b74952cfa1"></a>`module` | "stove0_review_target_support" |
| <a id="s-6b0b03ea44"></a>`name` | "ReviewTargetConfig" |
| <a id="s-38ac975c44"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_review_target_support.ReviewTargetConfig.canonical_samplers](stove0-review-target-support-reviewtargetconfig-canonical-samplers.md)

## Governing policies

- <a id="pa-1c1985e416"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — `reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_support.ReviewTargetConfig`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1ef2f392a9829e8b414d990221b24731ead8e1d8ce15ccf15bcec23a0e93eb4a -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "465ba4684267b5a403b655bcbaaa58dbe51e259b14ffa6dc2fbd902af816ff0c",
    "signature": "'(*, samplers: Annotated[tuple[stove0_review_target_support.app.SamplerConfig, ...], MinLen(min_length=1)]) -> None'"
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "ReviewTargetConfig",
  "unit": "export"
}
```
