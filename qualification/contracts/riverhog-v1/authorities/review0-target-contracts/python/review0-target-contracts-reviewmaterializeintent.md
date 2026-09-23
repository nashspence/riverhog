# review0_target_contracts.ReviewMaterializeIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-reviewmaterializeintent:de836164f3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-59c4481943"></a>
- <a id="s-2261e1aae2"></a>`distribution`: `review0-target-contracts`
- <a id="s-343c52ff35"></a>`module`: `review0_target_contracts`
- <a id="s-baea6f9a0c"></a>`name`: `ReviewMaterializeIntent`
- <a id="s-d02cda693f"></a>`unit`: `export`

### Declared structure

- <a id="s-6ffa0a0725"></a>`kind`: `"class"`
- <a id="s-98c4c478ea"></a>`signature`: `"'(*, sample_plan: review0_target_contracts.models.ReviewSamplePlan, variant: review0_target_contracts.models.ReviewVariantIntent) -> None'"`

#### Validated model schema

<a id="s-e1b8433a5b"></a>

- <a id="s-f6496cefc7"></a>`type`: `"object"`
- <a id="s-8b230e64d1"></a>`additionalProperties`: `false`
- <a id="s-8bc5b2ae8a"></a>`required`: `["sample_plan","variant"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-828b16e70c"></a>`sample_plan` | yes | [ReviewSamplePlan](#s-f25f55be88) |  |
| <a id="s-f07119407a"></a>`variant` | yes | [ReviewVariantIntent](#s-694c664c4a) |  |

##### Definitions

- [JsonValue](#s-57c4c73b99)
- [ReviewSamplePlan](#s-f25f55be88)
- [ReviewSampleWindow](#s-4492fabe3d)
- [ReviewVariantIntent](#s-694c664c4a)

##### <a id="s-57c4c73b99"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-f25f55be88"></a>definition `ReviewSamplePlan`

- <a id="s-19e259615e"></a>`type`: `"object"`
- <a id="s-74b8de387a"></a>`additionalProperties`: `false`
- <a id="s-30f3af50d1"></a>`required`: `["samples_per_artifact","window_duration_ms","windows","sample_plan_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-be02a78437"></a>`format` | no | type="string"; const="review0-sample-plan/v1"; default="review0-sample-plan/v1" |  |
| <a id="s-ddf6bd9ac4"></a>`sample_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1f7f221693"></a>`samples_per_artifact` | yes | type="integer"; minimum=1 |  |
| <a id="s-0b3181c5b6"></a>`selection_method` | no | type="string"; const="evenly-spaced/v1"; default="evenly-spaced/v1" |  |
| <a id="s-6125892755"></a>`window_duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-bd11aa9cfd"></a>`windows` | yes | type="array"; items=([ReviewSampleWindow](#s-4492fabe3d)); minItems=1 |  |

##### <a id="s-4492fabe3d"></a>definition `ReviewSampleWindow`

- <a id="s-0058190f02"></a>`type`: `"object"`
- <a id="s-00c0890b04"></a>`additionalProperties`: `false`
- <a id="s-dac1bf55c7"></a>`required`: `["artifact_id","start_ms","duration_ms"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c282f6dc2d"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-186942aa2e"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-edab850f33"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

##### <a id="s-694c664c4a"></a>definition `ReviewVariantIntent`

- <a id="s-7164ebfd65"></a>`type`: `"object"`
- <a id="s-b14c722c14"></a>`additionalProperties`: `false`
- <a id="s-011633ea8e"></a>`required`: `["id","portable_intent"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-703dce330b"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e934a2d6aa"></a>`portable_intent` | yes | type="object"; additionalProperties=([JsonValue](#s-57c4c73b99)) |  |

## Governing policies

- <a id="pa-f3ffb755ff"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.ReviewMaterializeIntent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 48cd207d6f8b522bf1fb2b1f94266b721b391b984f4c830fcfae704cf4c5695d -->

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
    "signature": "'(*, sample_plan: review0_target_contracts.models.ReviewSamplePlan, variant: review0_target_contracts.models.ReviewVariantIntent) -> None'"
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "ReviewMaterializeIntent",
  "unit": "export"
}
```

</details>
