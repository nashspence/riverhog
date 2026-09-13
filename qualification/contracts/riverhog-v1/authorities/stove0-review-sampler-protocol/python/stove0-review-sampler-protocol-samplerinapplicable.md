# stove0_review_sampler_protocol.SamplerInapplicable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerinapplicable:eed8d6dbea -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a0621baa7d"></a>
| Field | Shape |
|---|---|
| <a id="s-9de87ceea4"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-0b8cdcd6d6"></a>`distribution` | "stove0-review-sampler-protocol" |
| <a id="s-04ab2173bc"></a>`module` | "stove0_review_sampler_protocol" |
| <a id="s-955c6ca23c"></a>`name` | "SamplerInapplicable" |
| <a id="s-551f4f6e32"></a>`unit` | "export" |

## Governing policies

- <a id="pa-341c6bb670"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerInapplicable`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bb354d7fdeb66da25d3f80b0b463825f3ef4ba860d8642cbee46e0749131f675 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "a5e2ccae9cf0dfad46fa94f700c6994dcc8fe643c1ae44cd945353e8d765ef51",
    "signature": "\"(*, code: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerInapplicable",
  "unit": "export"
}
```
