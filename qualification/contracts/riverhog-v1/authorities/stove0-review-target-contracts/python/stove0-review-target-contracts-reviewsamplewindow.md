# stove0_review_target_contracts.ReviewSampleWindow

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-reviewsamplewindow:980553191b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8e272d11c5"></a>
| Field | Shape |
|---|---|
| <a id="s-69a3f11491"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-548d08bb07"></a>`distribution` | "stove0-review-target-contracts" |
| <a id="s-9c4ed66da6"></a>`module` | "stove0_review_target_contracts" |
| <a id="s-2b8c2e73b3"></a>`name` | "ReviewSampleWindow" |
| <a id="s-b117aff8b3"></a>`unit` | "export" |

## Governing policies

- <a id="pa-5576f89b37"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — `reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.ReviewSampleWindow`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a2f3becabf99d28927fa705c4d7d64cda7a6b3bd20bc86edda5c9bb8150924db -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "fce3a584b3ca9e6a8ba5fe4f91d261c9c18a807411dc26d26f74d144cdfe18d1",
    "signature": "'(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], start_ms: Annotated[int, Ge(ge=0)], duration_ms: Annotated[int, Ge(ge=1)]) -> None'"
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "ReviewSampleWindow",
  "unit": "export"
}
```
