# review0_target_contracts.ReviewSamplePlanPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-reviewsampleplanpayload:cc8a7aab28 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bbac072318"></a>
- <a id="s-7c9d181819"></a>`distribution`: `review0-target-contracts`
- <a id="s-4b26360a49"></a>`module`: `review0_target_contracts`
- <a id="s-8837634b34"></a>`name`: `ReviewSamplePlanPayload`
- <a id="s-d96eb6cc19"></a>`unit`: `export`

### Declared structure

- <a id="s-053fbaa985"></a>`kind`: `"class"`
- <a id="s-3bce9fe8f9"></a>`signature`: `"\"(*, format: Literal['review0-sample-plan/v1'] = 'review0-sample-plan/v1', selection_method: Literal['evenly-spaced/v1'] = 'evenly-spaced/v1', samples_per_artifact: Annotated[int, Ge(ge=1)], window_duration_ms: Annotated[int, Ge(ge=1)], windows: Annotated[tuple[review0_target_contracts.models.ReviewSampleWindow, ...], MinLen(min_length=1)]) -> None\""`

#### Validated model schema

<a id="s-85993ae354"></a>

- <a id="s-f423726863"></a>`type`: `"object"`
- <a id="s-af939ad3e5"></a>`additionalProperties`: `false`
- <a id="s-8b78f0b638"></a>`required`: `["samples_per_artifact","window_duration_ms","windows"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ac8c0f9235"></a>`format` | no | type="string"; const="review0-sample-plan/v1"; default="review0-sample-plan/v1" |  |
| <a id="s-4e5c989b7f"></a>`samples_per_artifact` | yes | type="integer"; minimum=1 |  |
| <a id="s-5efdff6ba7"></a>`selection_method` | no | type="string"; const="evenly-spaced/v1"; default="evenly-spaced/v1" |  |
| <a id="s-c120d335f3"></a>`window_duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-51856257ee"></a>`windows` | yes | type="array"; items=([ReviewSampleWindow](#s-68b68a6557)); minItems=1 |  |

##### Definitions

- [ReviewSampleWindow](#s-68b68a6557)

##### <a id="s-68b68a6557"></a>definition `ReviewSampleWindow`

- <a id="s-375c616106"></a>`type`: `"object"`
- <a id="s-a024f1288e"></a>`additionalProperties`: `false`
- <a id="s-00e7159d8c"></a>`required`: `["artifact_id","start_ms","duration_ms"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-56798d3b81"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-c3b22536aa"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-f3a996232b"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Related interface records

- [canonical_windows](review0-target-contracts-reviewsampleplanpayload-canonical-windows.md)
- [exact_declared_shape](review0-target-contracts-reviewsampleplanpayload-exact-declared-shape.md)

## Governing policies

- <a id="pa-e70ca33fb4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.ReviewSamplePlanPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8d6b01cd227af418463e0f2063109b2635e78149ae4de2f5eeefaa87ed01c508 -->

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
    "signature": "\"(*, format: Literal['review0-sample-plan/v1'] = 'review0-sample-plan/v1', selection_method: Literal['evenly-spaced/v1'] = 'evenly-spaced/v1', samples_per_artifact: Annotated[int, Ge(ge=1)], window_duration_ms: Annotated[int, Ge(ge=1)], windows: Annotated[tuple[review0_target_contracts.models.ReviewSampleWindow, ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "ReviewSamplePlanPayload",
  "unit": "export"
}
```

</details>
