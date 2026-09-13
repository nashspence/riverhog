# stove0_review_target_contracts.ReviewMaterializeIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-reviewmate-2d5ab8bf68:fcde7a0d02 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9727e13bbf"></a>
| Field | Shape |
|---|---|
| <a id="s-bfabbfbab1"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-2859aeda44"></a>`distribution` | "stove0-review-target-contracts" |
| <a id="s-7959551eb4"></a>`module` | "stove0_review_target_contracts" |
| <a id="s-f460554b56"></a>`name` | "ReviewMaterializeIntent" |
| <a id="s-b17b1a1f04"></a>`unit` | "export" |

## Governing policies

- <a id="pa-e49a257dfe"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — `reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.ReviewMaterializeIntent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 89dcebf2b76452e9fcbf60032956dbaa5705a6add84acbf52bfc2ec0002badf3 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "4f40aec33b76e3333c7c3860975aebee92e0f66bf1eb7310b9f734a9b497b706",
    "signature": "'(*, sample_plan: stove0_review_target_contracts.models.ReviewSamplePlan, variant: stove0_review_target_contracts.models.ReviewVariantIntent) -> None'"
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "ReviewMaterializeIntent",
  "unit": "export"
}
```
