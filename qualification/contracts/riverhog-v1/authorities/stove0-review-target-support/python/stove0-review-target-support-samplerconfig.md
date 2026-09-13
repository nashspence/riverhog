# stove0_review_target_support.SamplerConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-samplerconfig:1d8a4f3b6a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ec7dd37bb4"></a>
| Field | Shape |
|---|---|
| <a id="s-9dcf968b9e"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-64576c57cd"></a>`distribution` | "stove0-review-target-support" |
| <a id="s-5fa4ba6f2f"></a>`module` | "stove0_review_target_support" |
| <a id="s-3c67516718"></a>`name` | "SamplerConfig" |
| <a id="s-6e71f4a193"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_review_target_support.SamplerConfig.absolute_token_file](stove0-review-target-support-samplerconfig-absolute-token-file.md)

## Governing policies

- <a id="pa-fb626c40e8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — `reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_support.SamplerConfig`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: defe06f7513ba569ff683ccbb7c9c111962b3998472a97bf0b54a0a38073d084 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "f41a6e541ec173a2e5d15cf28386e186b3440a8e7345c69f22509c6add3dad8e",
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$')], base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], token_file: pathlib.Path, allow_insecure_http: bool = False, descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], image_digest: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "SamplerConfig",
  "unit": "export"
}
```
