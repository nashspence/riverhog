# stove0_review_sampler_protocol.SamplerResultPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerresultpayload:a978d5723e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b19295588e"></a>
| Field | Shape |
|---|---|
| <a id="s-ab9852c9eb"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-4b70d85692"></a>`distribution` | "stove0-review-sampler-protocol" |
| <a id="s-3ccf3e8b68"></a>`module` | "stove0_review_sampler_protocol" |
| <a id="s-79281dffe7"></a>`name` | "SamplerResultPayload" |
| <a id="s-5a3cfc3f87"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_protocol.SamplerResultPayload.state_shape](stove0-review-sampler-protocol-samplerresultpayload-state-shape.md)
- [stove0_review_sampler_protocol.SamplerResultPayload.canonical_outputs](stove0-review-sampler-protocol-samplerresultpayload-canonical-outputs.md)

## Governing policies

- <a id="pa-e3228be7cc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerResultPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1184de667b60d6a44d19442f025ec9baf5eb83ad42395a0ed320b6dd0e32fda5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "43995295819eee9d7834b9a88b848ddce3aa1e525b17ab129b0571c2eaed661b",
    "signature": "\"(*, format: Literal['stove0-review-sampler-result/v1'] = 'stove0-review-sampler-result/v1', request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], state: Literal['succeeded', 'inapplicable', 'failed', 'canceled'], outputs: tuple[stove0_review_sampler_protocol.SamplerOutput, ...] = (), execution_evidence: dict[str, JsonValue] = <factory>, failure: stove0_review_sampler_protocol.SamplerFailure | None = None, inapplicable: stove0_review_sampler_protocol.SamplerInapplicable | None = None) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerResultPayload",
  "unit": "export"
}
```
