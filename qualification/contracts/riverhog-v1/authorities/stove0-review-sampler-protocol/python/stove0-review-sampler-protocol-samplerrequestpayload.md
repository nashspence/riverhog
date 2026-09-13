# stove0_review_sampler_protocol.SamplerRequestPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerrequestpayload:a0a1dd4a83 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0352e93d68"></a>
| Field | Shape |
|---|---|
| <a id="s-d882fe9d9c"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-30ef560e69"></a>`distribution` | "stove0-review-sampler-protocol" |
| <a id="s-a6ef43ebde"></a>`module` | "stove0_review_sampler_protocol" |
| <a id="s-55f65755cf"></a>`name` | "SamplerRequestPayload" |
| <a id="s-4dab40d57d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_protocol.SamplerRequestPayload.canonical_cancellation_path](stove0-review-sampler-protocol-samplerrequestpayload-canonical-cancellation-path.md)
- [stove0_review_sampler_protocol.SamplerRequestPayload.references_exact_inputs](stove0-review-sampler-protocol-samplerrequestpayload-references-exact-inputs.md)
- [stove0_review_sampler_protocol.SamplerRequestPayload.canonical_inputs](stove0-review-sampler-protocol-samplerrequestpayload-canonical-inputs.md)
- [stove0_review_sampler_protocol.SamplerRequestPayload.canonical_windows](stove0-review-sampler-protocol-samplerrequestpayload-canonical-windows.md)

## Governing policies

- <a id="pa-605e71c243"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerRequestPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 111abc7a7cc060023e1c4a0307359feead0a0996a0e0abe5c8ed8d29a5a78373 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "72ab9541c3c2ca5d9d11df63bda09ee21d19c7d0f7aa7305749271d6162ac01d",
    "signature": "\"(*, format: Literal['stove0-review-sampler-request/v1'] = 'stove0-review-sampler-request/v1', sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], workspace_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: Annotated[tuple[stove0_review_sampler_protocol.SamplerInput, ...], MinLen(min_length=1)], windows: Annotated[tuple[stove0_review_sampler_protocol.SamplerWindow, ...], MinLen(min_length=1)], portable_intent: dict[str, JsonValue], maximum_output_bytes: Annotated[int, Ge(ge=1), Le(le=1099511627776)], timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)], cancellation_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)]) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerRequestPayload",
  "unit": "export"
}
```
