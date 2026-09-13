# stove0_review_sampler_protocol.SamplerWindow

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerwindow:2500c294f3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f0d20e20a8"></a>
| Field | Shape |
|---|---|
| <a id="s-6bed19425f"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-e21618d727"></a>`distribution` | "stove0-review-sampler-protocol" |
| <a id="s-effc939627"></a>`module` | "stove0_review_sampler_protocol" |
| <a id="s-e476694ee5"></a>`name` | "SamplerWindow" |
| <a id="s-933428f98e"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_protocol.SamplerWindow.canonical_output_path](stove0-review-sampler-protocol-samplerwindow-canonical-output-path.md)

## Governing policies

- <a id="pa-cc1e4ecb10"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerWindow`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e2d7504608f5eb48855be0eb450ad869b19779136e2df5902d5b7a4bd65e11cf -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "70e05026e2bb99cdfa6d74df5cc6bde72c26a3df7e62152f80345fed5a8d4420",
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], input_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], start_ms: Annotated[int, Ge(ge=0)], duration_ms: Annotated[int, Ge(ge=1)], output_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)]) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerWindow",
  "unit": "export"
}
```
