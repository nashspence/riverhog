# stove0_review_target_contracts.ReviewSamplePlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-reviewsampleplan:bcbc679873 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c6cdc8f921"></a>
- <a id="s-21422ef3c8"></a>`distribution`: `stove0-review-target-contracts`
- <a id="s-9bc19bffa7"></a>`module`: `stove0_review_target_contracts`
- <a id="s-df89e02872"></a>`name`: `ReviewSamplePlan`
- <a id="s-f232ea82b5"></a>`unit`: `export`

### Declared structure

- <a id="s-1162e0139c"></a>`kind`: `"class"`
- <a id="s-b9ace47c9e"></a>`signature`: `"\"(*, format: Literal['stove0-review-sample-plan/v1'] = 'stove0-review-sample-plan/v1', selection_method: Literal['evenly-spaced/v1'] = 'evenly-spaced/v1', samples_per_artifact: Annotated[int, Ge(ge=1)], window_duration_ms: Annotated[int, Ge(ge=1)], windows: Annotated[tuple[stove0_review_target_contracts.models.ReviewSampleWindow, ...], MinLen(min_length=1)], sample_plan_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-4ca26ad7c9"></a>
- <a id="s-baf248ccc8"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9abb9e9a13"></a>`format` | no | type="string"; const="stove0-review-sample-plan/v1" |  |
| <a id="s-6a67a5c239"></a>`sample_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-dfaacf6e89"></a>`samples_per_artifact` | yes | type="integer"; minimum=1 |  |
| <a id="s-ee4855cb3b"></a>`selection_method` | no | type="string"; const="evenly-spaced/v1" |  |
| <a id="s-4e3b1e1c7f"></a>`window_duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-efc1933cc1"></a>`windows` | yes | type="array"; minItems=1; items=(#/$defs/ReviewSampleWindow) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-28f5912f52"></a>`ReviewSampleWindow` | type="object"; fields=`artifact_id`, `duration_ms`, `start_ms`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_review_target_contracts.ReviewSamplePlan.verify_digest](stove0-review-target-contracts-reviewsampleplan-verify-digest.md)
- [stove0_review_target_contracts.ReviewSamplePlan.seal](stove0-review-target-contracts-reviewsampleplan-seal.md)

## Governing policies

- <a id="pa-f47cf88d91"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — `reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.ReviewSamplePlan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0ef4175979805d4744d01555e8bd6b452701c9dcbab2db5edbab1175c004fda9 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ReviewSampleWindow": {
          "additionalProperties": false,
          "properties": {
            "artifact_id": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            "duration_ms": {
              "minimum": 1,
              "type": "integer"
            },
            "start_ms": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "artifact_id",
            "start_ms",
            "duration_ms"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-review-sample-plan/v1",
          "default": "stove0-review-sample-plan/v1",
          "type": "string"
        },
        "sample_plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "samples_per_artifact": {
          "minimum": 1,
          "type": "integer"
        },
        "selection_method": {
          "const": "evenly-spaced/v1",
          "default": "evenly-spaced/v1",
          "type": "string"
        },
        "window_duration_ms": {
          "minimum": 1,
          "type": "integer"
        },
        "windows": {
          "items": {
            "$ref": "#/$defs/ReviewSampleWindow"
          },
          "minItems": 1,
          "type": "array"
        }
      },
      "required": [
        "samples_per_artifact",
        "window_duration_ms",
        "windows",
        "sample_plan_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-review-sample-plan/v1'] = 'stove0-review-sample-plan/v1', selection_method: Literal['evenly-spaced/v1'] = 'evenly-spaced/v1', samples_per_artifact: Annotated[int, Ge(ge=1)], window_duration_ms: Annotated[int, Ge(ge=1)], windows: Annotated[tuple[stove0_review_target_contracts.models.ReviewSampleWindow, ...], MinLen(min_length=1)], sample_plan_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "ReviewSamplePlan",
  "unit": "export"
}
```
