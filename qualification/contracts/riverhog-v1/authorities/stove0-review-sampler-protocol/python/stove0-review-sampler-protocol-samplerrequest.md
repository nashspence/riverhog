# stove0_review_sampler_protocol.SamplerRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerrequest:8b595fc3e7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-98a9b591fa"></a>
| Field | Shape |
|---|---|
| <a id="s-50a51ea0da"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-c897f06c0a"></a>`distribution` | "stove0-review-sampler-protocol" |
| <a id="s-de46c5a853"></a>`module` | "stove0_review_sampler_protocol" |
| <a id="s-af698aeef5"></a>`name` | "SamplerRequest" |
| <a id="s-002225b5b8"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_protocol.SamplerRequest.verify_digest](stove0-review-sampler-protocol-samplerrequest-verify-digest.md)
- [stove0_review_sampler_protocol.SamplerRequest.seal](stove0-review-sampler-protocol-samplerrequest-seal.md)

## Governing policies

- <a id="pa-b5ff25d95a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7b935ba6c1db4b3507c2c0ba42959ee385b63cc9ec5cd1304eafedd8152863d6 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "c4c0c1f6ca2eeb83c6b5bb7efc469587b92ba815ce556e33a976a98b0d093c9f",
    "signature": "\"(*, format: Literal['stove0-review-sampler-request/v1'] = 'stove0-review-sampler-request/v1', sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], workspace_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: Annotated[tuple[stove0_review_sampler_protocol.SamplerInput, ...], MinLen(min_length=1)], windows: Annotated[tuple[stove0_review_sampler_protocol.SamplerWindow, ...], MinLen(min_length=1)], portable_intent: dict[str, JsonValue], maximum_output_bytes: Annotated[int, Ge(ge=1), Le(le=1099511627776)], timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)], cancellation_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerRequest",
  "unit": "export"
}
```
