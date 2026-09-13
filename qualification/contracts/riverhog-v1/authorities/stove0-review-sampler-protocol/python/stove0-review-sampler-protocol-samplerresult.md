# stove0_review_sampler_protocol.SamplerResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerresult:22baf9c97e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cfd5b77ea0"></a>
| Field | Shape |
|---|---|
| <a id="s-44d8066cae"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-58cef6fb15"></a>`distribution` | "stove0-review-sampler-protocol" |
| <a id="s-bea57ff4d1"></a>`module` | "stove0_review_sampler_protocol" |
| <a id="s-5052e9afe7"></a>`name` | "SamplerResult" |
| <a id="s-981a05f65c"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_protocol.SamplerResult.verify_digest](stove0-review-sampler-protocol-samplerresult-verify-digest.md)
- [stove0_review_sampler_protocol.SamplerResult.seal](stove0-review-sampler-protocol-samplerresult-seal.md)

## Governing policies

- <a id="pa-7194fe96cf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8c553c9954a7e754c81c9ec36e72552c9de2d621f60e574450d77ff5f3f560e2 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "21f6b9d3f23551988794e5c55d6db07545900c3d9b5dfc7b2372acc553d8e0d3",
    "signature": "\"(*, format: Literal['stove0-review-sampler-result/v1'] = 'stove0-review-sampler-result/v1', request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], state: Literal['succeeded', 'inapplicable', 'failed', 'canceled'], outputs: tuple[stove0_review_sampler_protocol.SamplerOutput, ...] = (), execution_evidence: dict[str, JsonValue] = <factory>, failure: stove0_review_sampler_protocol.SamplerFailure | None = None, inapplicable: stove0_review_sampler_protocol.SamplerInapplicable | None = None, result_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerResult",
  "unit": "export"
}
```
