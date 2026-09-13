# stove0_review_target_contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts:de32682b1e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-675e4113a1"></a>
| Field | Shape |
|---|---|
| <a id="s-8d2895fa99"></a>`candidate_id` | "python:stove0-review-target-contracts:stove0_review_target_contracts" |
| <a id="s-9dc108852d"></a>`distribution` | "stove0-review-target-contracts" |
| <a id="s-a6d9f62dd4"></a>`exports` | additional keys=`REVIEW_AUDIO_ROLE`, `REVIEW_INDEX_ROLE`, `REVIEW_MATERIALIZE_INTENT_CONFORMANCE_VECTORS`, `REVIEW_MATERIALIZE_INTENT_SCHEMA`, `REVIEW_MATERIALIZE_INTENT_SCHEMA_ID`, `REVIEW_MATERIALIZE_INTENT_SEMANTICS`, `REVIEW_MATERIALIZE_OPERATION`, `REVIEW_MATERIALIZE_OPERATION_ID`, `REVIEW_RCLONE_DELIVER_OPERATION`, `REVIEW_RCLONE_DELIVER_OPERATION_ID`, `REVIEW_RCLONE_RECEIPT_SCHEMA`, `REVIEW_RCLONE_RECEIPT_SCHEMA_ID`, `REVIEW_SOURCE_ROLE`, `REVIEW_VIDEO_ROLE`, `ReviewMaterializeIntent`, `ReviewSamplePlan`, `ReviewSamplePlanPayload`, `ReviewSampleWindow`, `ReviewVariantIntent`, `validate_review_materialize_intent` |
| <a id="s-a07246a0f4"></a>`module` | "stove0_review_target_contracts" |

## Governing policies

- <a id="pa-7122ad5ccf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — `reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/55`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ce3735e266c86b7ab17b816cc7a92a195942f99867c2a1f01c59bf7ead0cfdab -->

```json
{
  "candidate_id": "python:stove0-review-target-contracts:stove0_review_target_contracts",
  "distribution": "stove0-review-target-contracts",
  "exports": {
    "REVIEW_AUDIO_ROLE": {
      "kind": "constant",
      "value": "stove0.review.audio/v1"
    },
    "REVIEW_INDEX_ROLE": {
      "kind": "constant",
      "value": "stove0.review.index/v1"
    },
    "REVIEW_MATERIALIZE_INTENT_CONFORMANCE_VECTORS": {
      "kind": "object",
      "type": "stove0_target_protocol.conformance.SemanticIntentConformanceVectors"
    },
    "REVIEW_MATERIALIZE_INTENT_SCHEMA": {
      "kind": "object",
      "type": "stove0_protocol.models.JsonSchemaDocument"
    },
    "REVIEW_MATERIALIZE_INTENT_SCHEMA_ID": {
      "kind": "constant",
      "value": "stove0.review.materialize-intent/v1"
    },
    "REVIEW_MATERIALIZE_INTENT_SEMANTICS": {
      "kind": "object",
      "type": "stove0_protocol.models.SemanticValidationProfile"
    },
    "REVIEW_MATERIALIZE_OPERATION": {
      "kind": "object",
      "type": "stove0_target_protocol.protocol.OperationContract"
    },
    "REVIEW_MATERIALIZE_OPERATION_ID": {
      "kind": "constant",
      "value": "stove0.review.materialize/v1"
    },
    "REVIEW_RCLONE_DELIVER_OPERATION": {
      "kind": "object",
      "type": "stove0_target_protocol.protocol.OperationContract"
    },
    "REVIEW_RCLONE_DELIVER_OPERATION_ID": {
      "kind": "constant",
      "value": "stove0.review.rclone-deliver/v1"
    },
    "REVIEW_RCLONE_RECEIPT_SCHEMA": {
      "kind": "object",
      "type": "stove0_protocol.models.JsonSchemaDocument"
    },
    "REVIEW_RCLONE_RECEIPT_SCHEMA_ID": {
      "kind": "constant",
      "value": "stove0.review.rclone-receipt/v1"
    },
    "REVIEW_SOURCE_ROLE": {
      "kind": "constant",
      "value": "stove0.review.source/v1"
    },
    "REVIEW_VIDEO_ROLE": {
      "kind": "constant",
      "value": "stove0.review.video/v1"
    },
    "ReviewMaterializeIntent": {
      "kind": "class",
      "schema_sha256": "4f40aec33b76e3333c7c3860975aebee92e0f66bf1eb7310b9f734a9b497b706",
      "signature": "'(*, sample_plan: stove0_review_target_contracts.models.ReviewSamplePlan, variant: stove0_review_target_contracts.models.ReviewVariantIntent) -> None'"
    },
    "ReviewSamplePlan": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "\"(cls, payload: 'ReviewSamplePlanPayload') -> 'ReviewSamplePlan'\""
        },
        "verify_digest": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "300236b15485789a0e69f7afea9ac0bc549d8af384a88a3d5bf91dcd765ddd84",
      "signature": "\"(*, format: Literal['stove0-review-sample-plan/v1'] = 'stove0-review-sample-plan/v1', selection_method: Literal['evenly-spaced/v1'] = 'evenly-spaced/v1', samples_per_artifact: Annotated[int, Ge(ge=1)], window_duration_ms: Annotated[int, Ge(ge=1)], windows: Annotated[tuple[stove0_review_target_contracts.models.ReviewSampleWindow, ...], MinLen(min_length=1)], sample_plan_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
    },
    "ReviewSamplePlanPayload": {
      "kind": "class",
      "members": {
        "canonical_windows": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'tuple[ReviewSampleWindow, ...]') -> 'tuple[ReviewSampleWindow, ...]'\""
        },
        "exact_declared_shape": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "d44be71fa4e1e60918c17a8a6f73a93632ad718254b63595c6484604c69262f2",
      "signature": "\"(*, format: Literal['stove0-review-sample-plan/v1'] = 'stove0-review-sample-plan/v1', selection_method: Literal['evenly-spaced/v1'] = 'evenly-spaced/v1', samples_per_artifact: Annotated[int, Ge(ge=1)], window_duration_ms: Annotated[int, Ge(ge=1)], windows: Annotated[tuple[stove0_review_target_contracts.models.ReviewSampleWindow, ...], MinLen(min_length=1)]) -> None\""
    },
    "ReviewSampleWindow": {
      "kind": "class",
      "schema_sha256": "fce3a584b3ca9e6a8ba5fe4f91d261c9c18a807411dc26d26f74d144cdfe18d1",
      "signature": "'(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], start_ms: Annotated[int, Ge(ge=0)], duration_ms: Annotated[int, Ge(ge=1)]) -> None'"
    },
    "ReviewVariantIntent": {
      "kind": "class",
      "schema_sha256": "d26261ea6d361ae2b8625cc13db4d644be76e5e3746048643ecbdc2463708e92",
      "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$')], portable_intent: dict[str, JsonValue]) -> None\""
    },
    "validate_review_materialize_intent": {
      "kind": "function",
      "signature": "\"(intent: 'Mapping[str, object]') -> 'None'\""
    }
  },
  "module": "stove0_review_target_contracts"
}
```
