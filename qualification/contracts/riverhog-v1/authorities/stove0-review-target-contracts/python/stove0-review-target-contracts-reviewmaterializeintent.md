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
- <a id="s-2859aeda44"></a>`distribution`: `stove0-review-target-contracts`
- <a id="s-7959551eb4"></a>`module`: `stove0_review_target_contracts`
- <a id="s-f460554b56"></a>`name`: `ReviewMaterializeIntent`
- <a id="s-b17b1a1f04"></a>`unit`: `export`

### Declared structure

- <a id="s-7e8760c54f"></a>`kind`: `"class"`
- <a id="s-821bfdeeb7"></a>`signature`: `"'(*, sample_plan: stove0_review_target_contracts.models.ReviewSamplePlan, variant: stove0_review_target_contracts.models.ReviewVariantIntent) -> None'"`

#### Validated model schema

<a id="s-772b957e5e"></a>

- <a id="s-fa9a048ade"></a>`type`: `"object"`
- <a id="s-56f5d93696"></a>`additionalProperties`: `false`
- <a id="s-c9e4d4749f"></a>`required`: `["sample_plan","variant"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f792084b8d"></a>`sample_plan` | yes | [ReviewSamplePlan](#s-1e4346490e) |  |
| <a id="s-c1f5b36439"></a>`variant` | yes | [ReviewVariantIntent](#s-248948c57f) |  |

##### Definitions

- [JsonValue](#s-3508712c62)
- [ReviewSamplePlan](#s-1e4346490e)
- [ReviewSampleWindow](#s-f8858821b9)
- [ReviewVariantIntent](#s-248948c57f)

##### <a id="s-3508712c62"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-1e4346490e"></a>definition `ReviewSamplePlan`

- <a id="s-df2627b37f"></a>`type`: `"object"`
- <a id="s-789cfb152e"></a>`additionalProperties`: `false`
- <a id="s-5f3e798ad3"></a>`required`: `["samples_per_artifact","window_duration_ms","windows","sample_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1e64bd5178"></a>`format` | no | type="string"; const="stove0-review-sample-plan/v1"; default="stove0-review-sample-plan/v1" |  |
| <a id="s-a02bbed08c"></a>`sample_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c05732a77f"></a>`samples_per_artifact` | yes | type="integer"; minimum=1 |  |
| <a id="s-7f50d2fc68"></a>`selection_method` | no | type="string"; const="evenly-spaced/v1"; default="evenly-spaced/v1" |  |
| <a id="s-0be553bb8e"></a>`window_duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-da2e32dfb7"></a>`windows` | yes | type="array"; items=([ReviewSampleWindow](#s-f8858821b9)); minItems=1 |  |

##### <a id="s-f8858821b9"></a>definition `ReviewSampleWindow`

- <a id="s-2088947d5b"></a>`type`: `"object"`
- <a id="s-b098630f15"></a>`additionalProperties`: `false`
- <a id="s-dc44057dc0"></a>`required`: `["artifact_id","start_ms","duration_ms"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b69d27f6f7"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-0a6fe5a944"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-3b7849ca68"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

##### <a id="s-248948c57f"></a>definition `ReviewVariantIntent`

- <a id="s-797005ff8e"></a>`type`: `"object"`
- <a id="s-e9fd1a7007"></a>`additionalProperties`: `false`
- <a id="s-cb0686f035"></a>`required`: `["id","portable_intent"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-21c74193c8"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-464b0551e0"></a>`portable_intent` | yes | type="object"; additionalProperties=([JsonValue](#s-3508712c62)) |  |

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

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3cee3d6a854410497f8333a8ba9325d097ec06b22f3b42e7d60dec86112f1497 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonValue": {},
        "ReviewSamplePlan": {
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
        },
        "ReviewVariantIntent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "portable_intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            }
          },
          "required": [
            "id",
            "portable_intent"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "sample_plan": {
          "$ref": "#/$defs/ReviewSamplePlan"
        },
        "variant": {
          "$ref": "#/$defs/ReviewVariantIntent"
        }
      },
      "required": [
        "sample_plan",
        "variant"
      ],
      "type": "object"
    },
    "signature": "'(*, sample_plan: stove0_review_target_contracts.models.ReviewSamplePlan, variant: stove0_review_target_contracts.models.ReviewVariantIntent) -> None'"
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "ReviewMaterializeIntent",
  "unit": "export"
}
```

</details>
