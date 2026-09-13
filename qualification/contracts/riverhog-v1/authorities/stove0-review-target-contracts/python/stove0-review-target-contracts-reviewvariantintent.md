# stove0_review_target_contracts.ReviewVariantIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-reviewvariantintent:69ff991f2d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ffae67ccc1"></a>
| Field | Shape |
|---|---|
| <a id="s-d8402f5a9a"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-6ef4539618"></a>`distribution` | "stove0-review-target-contracts" |
| <a id="s-11f199b36d"></a>`module` | "stove0_review_target_contracts" |
| <a id="s-396f300eb6"></a>`name` | "ReviewVariantIntent" |
| <a id="s-6b17f90506"></a>`unit` | "export" |

## Governing policies

- <a id="pa-062aea031f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — `reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.ReviewVariantIntent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 09cbb9e33ce096b9f4ea72415efb43c1d3ab311445bd37941573b3bff31f34e7 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "d26261ea6d361ae2b8625cc13db4d644be76e5e3746048643ecbdc2463708e92",
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$')], portable_intent: dict[str, JsonValue]) -> None\""
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "ReviewVariantIntent",
  "unit": "export"
}
```
