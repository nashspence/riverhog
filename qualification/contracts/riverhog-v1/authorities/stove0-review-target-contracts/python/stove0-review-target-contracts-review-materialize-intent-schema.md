# stove0_review_target_contracts.REVIEW_MATERIALIZE_INTENT_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-review-mat-302f820dc0:cf6cd555f1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1bafa442ee"></a>
| Field | Shape |
|---|---|
| <a id="s-36a6039f28"></a>`contract` | type="stove0_protocol.models.JsonSchemaDocument"; additional keys=`kind` |
| <a id="s-f596e6c8fa"></a>`distribution` | "stove0-review-target-contracts" |
| <a id="s-145424d765"></a>`module` | "stove0_review_target_contracts" |
| <a id="s-eed2211c4f"></a>`name` | "REVIEW_MATERIALIZE_INTENT_SCHEMA" |
| <a id="s-ad967fbb74"></a>`unit` | "export" |

## Governing policies

- <a id="pa-da3290fb25"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — `reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.REVIEW_MATERIALIZE_INTENT_SCHEMA`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5aee3f4b79c73ec593aec0d73a0061815e6f983a4b7d17d6f9f16348f30cb122 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_protocol.models.JsonSchemaDocument"
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "REVIEW_MATERIALIZE_INTENT_SCHEMA",
  "unit": "export"
}
```
