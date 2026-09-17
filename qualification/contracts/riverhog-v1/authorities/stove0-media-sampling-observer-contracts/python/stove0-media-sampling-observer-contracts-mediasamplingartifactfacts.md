# stove0_media_sampling_observer_contracts.MediaSamplingArtifactFacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-sampling-observer-contracts:stove0-media-sampling-observer-contracts-79fcd99c7d:ec2fd28ce6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-sampling-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-95c516e5d7"></a>
- <a id="s-047d3862a0"></a>`distribution`: `stove0-media-sampling-observer-contracts`
- <a id="s-8a2ed58ca1"></a>`module`: `stove0_media_sampling_observer_contracts`
- <a id="s-5a0fa7462f"></a>`name`: `MediaSamplingArtifactFacts`
- <a id="s-3fe2f01d71"></a>`unit`: `export`

### Declared structure

- <a id="s-d1fc00fd16"></a>`kind`: `"class"`
- <a id="s-1b9d395755"></a>`signature`: `"'(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], duration_ms: Annotated[int, Ge(ge=1)], sampleable_ranges: Annotated[tuple[stove0_media_sampling_observer_contracts.contracts.SampleableRange, ...], MinLen(min_length=1)]) -> None'"`

#### Validated model schema

<a id="s-055aabb9f1"></a>

- <a id="s-210fa9b30f"></a>`type`: `"object"`
- <a id="s-be8c626844"></a>`additionalProperties`: `false`
- <a id="s-0b7fd6b0d4"></a>`required`: `["artifact_id","duration_ms","sampleable_ranges"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f7db6f3212"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-946d98127a"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-1353a5afe9"></a>`sampleable_ranges` | yes | type="array"; items=([SampleableRange](#s-01429a538a)); minItems=1 |  |

##### Definitions

- [SampleableRange](#s-01429a538a)

##### <a id="s-01429a538a"></a>definition `SampleableRange`

- <a id="s-c448a4043b"></a>`type`: `"object"`
- <a id="s-8de3b353fd"></a>`additionalProperties`: `false`
- <a id="s-512d5823c6"></a>`required`: `["start_ms","duration_ms"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4d8e770103"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-4792d95748"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Related interface records

- [validate_ranges](stove0-media-sampling-observer-contracts-mediasamplingartifactfacts-validate-ranges.md)

## Governing policies

- <a id="pa-487bf5145c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-media-sampling-observer-contracts:stove0_media_sampling_observer_contracts](../../../evidence/sources/authorities.md#src-b9344d06d7) — [reference/stove0/observers/contracts/media-sampling/src/stove0\_media\_sampling\_observer\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/observers/contracts/media-sampling/src/stove0_media_sampling_observer_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_media_sampling_observer_contracts.MediaSamplingArtifactFacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5ea555ad5d4af235f4164ced8ba8fbe3aa86ca82be4ec1273f75f14f2a86ba96 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "SampleableRange": {
          "additionalProperties": false,
          "properties": {
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
            "start_ms",
            "duration_ms"
          ],
          "type": "object"
        }
      },
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
        "sampleable_ranges": {
          "items": {
            "$ref": "#/$defs/SampleableRange"
          },
          "minItems": 1,
          "type": "array"
        }
      },
      "required": [
        "artifact_id",
        "duration_ms",
        "sampleable_ranges"
      ],
      "type": "object"
    },
    "signature": "'(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], duration_ms: Annotated[int, Ge(ge=1)], sampleable_ranges: Annotated[tuple[stove0_media_sampling_observer_contracts.contracts.SampleableRange, ...], MinLen(min_length=1)]) -> None'"
  },
  "distribution": "stove0-media-sampling-observer-contracts",
  "module": "stove0_media_sampling_observer_contracts",
  "name": "MediaSamplingArtifactFacts",
  "unit": "export"
}
```

</details>
