# stove0_review_sampler_protocol.SamplerFailure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerfailure:9c1b876dea -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0fbefc0203"></a>
| Field | Shape |
|---|---|
| <a id="s-f50f30f37e"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-237cd126d1"></a>`distribution` | "stove0-review-sampler-protocol" |
| <a id="s-136ca337d9"></a>`module` | "stove0_review_sampler_protocol" |
| <a id="s-9b85d292b1"></a>`name` | "SamplerFailure" |
| <a id="s-e5a35d6939"></a>`unit` | "export" |

## Governing policies

- <a id="pa-1d8898e259"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerFailure`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 38dc0b1f511452e3032b8cbc87b97881c47efd99d2cf202f89cf27ad298b8403 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "463469db3cd89d3a6b04230ebb807a8b96ffdae48c5784517cce987ca9fe7ce5",
    "signature": "\"(*, code: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerFailure",
  "unit": "export"
}
```
