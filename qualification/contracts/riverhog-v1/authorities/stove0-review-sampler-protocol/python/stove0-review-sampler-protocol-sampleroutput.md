# stove0_review_sampler_protocol.SamplerOutput

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-sampleroutput:fdddc22526 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-68bc0bed73"></a>
| Field | Shape |
|---|---|
| <a id="s-af9cedbee1"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-2539bfcd6a"></a>`distribution` | "stove0-review-sampler-protocol" |
| <a id="s-814ef07be9"></a>`module` | "stove0_review_sampler_protocol" |
| <a id="s-23416d7d34"></a>`name` | "SamplerOutput" |
| <a id="s-cbc124711e"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_protocol.SamplerOutput.canonical_sources](stove0-review-sampler-protocol-sampleroutput-canonical-sources.md)
- [stove0_review_sampler_protocol.SamplerOutput.canonical_path](stove0-review-sampler-protocol-sampleroutput-canonical-path.md)

## Governing policies

- <a id="pa-21da62be9c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerOutput`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 02cd0958de0209b4278a2670d917166a495b0c341096920346f6fea4088da370 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "6e4a50dfd5fcaa493a6baa6f34436ab5b4bc32ab808f0f2c7d6f99fda3befdac",
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], media_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], derived_from: Annotated[tuple[str, ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerOutput",
  "unit": "export"
}
```
