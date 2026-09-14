# stove0_review_target_contracts.ReviewSamplePlanPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-reviewsamp-e2c24613bd:6cabb8cdb5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c26a2b702f"></a>
- <a id="s-7a724c46a7"></a>`distribution`: `stove0-review-target-contracts`
- <a id="s-adf911076e"></a>`module`: `stove0_review_target_contracts`
- <a id="s-9e73fecc70"></a>`name`: `ReviewSamplePlanPayload`
- <a id="s-e478ca86a6"></a>`unit`: `export`

### Declared structure

- <a id="s-008c3201a8"></a>`kind`: `"class"`
- <a id="s-32a2d106b4"></a>`signature`: `"\"(*, format: Literal['stove0-review-sample-plan/v1'] = 'stove0-review-sample-plan/v1', selection_method: Literal['evenly-spaced/v1'] = 'evenly-spaced/v1', samples_per_artifact: Annotated[int, Ge(ge=1)], window_duration_ms: Annotated[int, Ge(ge=1)], windows: Annotated[tuple[stove0_review_target_contracts.models.ReviewSampleWindow, ...], MinLen(min_length=1)]) -> None\""`

#### Validated model schema

<a id="s-2634f1ca5e"></a>
- <a id="s-edef076a79"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-39c68a4c3a"></a>`format` | no | type="string"; const="stove0-review-sample-plan/v1" |  |
| <a id="s-ba72ad0a30"></a>`samples_per_artifact` | yes | type="integer"; minimum=1 |  |
| <a id="s-251300e0e5"></a>`selection_method` | no | type="string"; const="evenly-spaced/v1" |  |
| <a id="s-959d642cbd"></a>`window_duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-29c539549f"></a>`windows` | yes | type="array"; minItems=1; items=(#/$defs/ReviewSampleWindow) |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-cd64eb8571"></a>`ReviewSampleWindow` | type="object"; fields=`artifact_id`, `duration_ms`, `start_ms`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [canonical_windows](stove0-review-target-contracts-reviewsampleplanpayload-canonical-windows.md)
- [exact_declared_shape](stove0-review-target-contracts-reviewsampleplanpayload-exact-declared-shape.md)

## Governing policies

- <a id="pa-455b87c5e5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources.md#src-1d0886e380) — `reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.ReviewSamplePlanPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d991d33f2c35af09faa9cac5f4ddf8488fefbe192eb4a292633fcaab10280244 -->

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
        "windows"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-review-sample-plan/v1'] = 'stove0-review-sample-plan/v1', selection_method: Literal['evenly-spaced/v1'] = 'evenly-spaced/v1', samples_per_artifact: Annotated[int, Ge(ge=1)], window_duration_ms: Annotated[int, Ge(ge=1)], windows: Annotated[tuple[stove0_review_target_contracts.models.ReviewSampleWindow, ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "ReviewSamplePlanPayload",
  "unit": "export"
}
```
