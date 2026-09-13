# stove0_review_sampler_protocol.SamplerInput

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerinput:0b92657807 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-13197b5a10"></a>
| Field | Shape |
|---|---|
| <a id="s-05b566d32c"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-5561a4b593"></a>`distribution` | "stove0-review-sampler-protocol" |
| <a id="s-a7923095b0"></a>`module` | "stove0_review_sampler_protocol" |
| <a id="s-3cc0349cf7"></a>`name` | "SamplerInput" |
| <a id="s-faefb8d72f"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_protocol.SamplerInput.canonical_path](stove0-review-sampler-protocol-samplerinput-canonical-path.md)

## Governing policies

- <a id="pa-093794773c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerInput`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5ab6d1852b8be7fb8b077131414b0ed93be62b78969313902e717deca44fa892 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "598e2ba7a086e6cdf7ff6d4260b002f766956002e655230031b8328b253ebd57",
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], media_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerInput",
  "unit": "export"
}
```
