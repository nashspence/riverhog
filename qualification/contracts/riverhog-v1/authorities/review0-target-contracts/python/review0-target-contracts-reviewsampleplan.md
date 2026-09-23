# review0_target_contracts.ReviewSamplePlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-reviewsampleplan:198f03395a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5a9de3d337"></a>
- <a id="s-bc04c965e3"></a>`distribution`: `review0-target-contracts`
- <a id="s-b9abc18496"></a>`module`: `review0_target_contracts`
- <a id="s-7d2e757aae"></a>`name`: `ReviewSamplePlan`
- <a id="s-f0c62953dc"></a>`unit`: `export`

### Declared structure

- <a id="s-ca236009f8"></a>`kind`: `"class"`
- <a id="s-c455241ea4"></a>`signature`: `"\"(*, format: Literal['review0-sample-plan/v1'] = 'review0-sample-plan/v1', selection_method: Literal['evenly-spaced/v1'] = 'evenly-spaced/v1', samples_per_artifact: Annotated[int, Ge(ge=1)], window_duration_ms: Annotated[int, Ge(ge=1)], windows: Annotated[tuple[review0_target_contracts.models.ReviewSampleWindow, ...], MinLen(min_length=1)], sample_plan_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-afbfb8b1f5"></a>

- <a id="s-66bc49827f"></a>`type`: `"object"`
- <a id="s-202758ffac"></a>`additionalProperties`: `false`
- <a id="s-89d51a81dd"></a>`required`: `["samples_per_artifact","window_duration_ms","windows","sample_plan_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b81d93875e"></a>`format` | no | type="string"; const="review0-sample-plan/v1"; default="review0-sample-plan/v1" |  |
| <a id="s-c0c95c42cd"></a>`sample_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-32dc2ee01b"></a>`samples_per_artifact` | yes | type="integer"; minimum=1 |  |
| <a id="s-472aa779e0"></a>`selection_method` | no | type="string"; const="evenly-spaced/v1"; default="evenly-spaced/v1" |  |
| <a id="s-ae1bca00aa"></a>`window_duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-937f73d86b"></a>`windows` | yes | type="array"; items=([ReviewSampleWindow](#s-36485fe65e)); minItems=1 |  |

##### Definitions

- [ReviewSampleWindow](#s-36485fe65e)

##### <a id="s-36485fe65e"></a>definition `ReviewSampleWindow`

- <a id="s-0c984363e5"></a>`type`: `"object"`
- <a id="s-0786b01663"></a>`additionalProperties`: `false`
- <a id="s-f972b1af6d"></a>`required`: `["artifact_id","start_ms","duration_ms"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7b29f1f367"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-976cf344c0"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-f4344104cc"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Related interface records

- [verify_digest](review0-target-contracts-reviewsampleplan-verify-digest.md)
- [exact_declared_shape](review0-target-contracts-reviewsampleplan-exact-declared-shape.md)
- [canonical_windows](review0-target-contracts-reviewsampleplan-canonical-windows.md)
- [seal](review0-target-contracts-reviewsampleplan-seal.md)

## Governing policies

- <a id="pa-d7b06dd33e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.ReviewSamplePlan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad8471514fba412bc79b34bbca89918b57050d362cd410373a897ede2364b7a7 -->

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
          "const": "review0-sample-plan/v1",
          "default": "review0-sample-plan/v1",
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
    "signature": "\"(*, format: Literal['review0-sample-plan/v1'] = 'review0-sample-plan/v1', selection_method: Literal['evenly-spaced/v1'] = 'evenly-spaced/v1', samples_per_artifact: Annotated[int, Ge(ge=1)], window_duration_ms: Annotated[int, Ge(ge=1)], windows: Annotated[tuple[review0_target_contracts.models.ReviewSampleWindow, ...], MinLen(min_length=1)], sample_plan_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "ReviewSamplePlan",
  "unit": "export"
}
```

</details>
