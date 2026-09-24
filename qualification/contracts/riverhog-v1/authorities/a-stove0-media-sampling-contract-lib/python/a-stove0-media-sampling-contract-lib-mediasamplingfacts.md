# a_stove0_media_sampling_contract_lib.MediaSamplingFacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-sampling-contract-lib:a-stove0-media-sampling-contract-lib-medi-16aae51735:038c323112 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-sampling-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-540fc035f0"></a>
- <a id="s-77fe7b3538"></a>`distribution`: `a-stove0-media-sampling-contract-lib`
- <a id="s-d739985d3e"></a>`module`: `a_stove0_media_sampling_contract_lib`
- <a id="s-84f5d99ef0"></a>`name`: `MediaSamplingFacts`
- <a id="s-31465c2529"></a>`unit`: `export`

### Declared structure

- <a id="s-92ee71f2b7"></a>`kind`: `"class"`
- <a id="s-ac8d65624c"></a>`signature`: `"'(*, artifacts: Annotated[tuple[a_stove0_media_sampling_contract_lib.contracts.MediaSamplingArtifactFacts, ...], MinLen(min_length=1)]) -> None'"`

#### Validated model schema

<a id="s-c256a3e6f7"></a>

- <a id="s-f0498f0a29"></a>`type`: `"object"`
- <a id="s-6b5865ddb9"></a>`additionalProperties`: `false`
- <a id="s-36458086b1"></a>`required`: `["artifacts"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-82c6487429"></a>`artifacts` | yes | type="array"; items=([MediaSamplingArtifactFacts](#s-00d6e3c0bb)); minItems=1 |  |

##### Definitions

- [MediaSamplingArtifactFacts](#s-00d6e3c0bb)
- [SampleableRange](#s-73498c87eb)

##### <a id="s-00d6e3c0bb"></a>definition `MediaSamplingArtifactFacts`

- <a id="s-900e4c16e3"></a>`type`: `"object"`
- <a id="s-06aa860b58"></a>`additionalProperties`: `false`
- <a id="s-aa63e626e5"></a>`required`: `["artifact_id","duration_ms","sampleable_ranges"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0a565a2459"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-fb296d1563"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-3571e19f26"></a>`sampleable_ranges` | yes | type="array"; items=([SampleableRange](#s-73498c87eb)); minItems=1 |  |

##### <a id="s-73498c87eb"></a>definition `SampleableRange`

- <a id="s-46d50c7b4a"></a>`type`: `"object"`
- <a id="s-93648eb997"></a>`additionalProperties`: `false`
- <a id="s-18bc641437"></a>`required`: `["start_ms","duration_ms"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fc2b6b56f9"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-74d59ab3e0"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Related interface records

- [canonical_artifacts](a-stove0-media-sampling-contract-lib-mediasamplingfacts-canonical-artifacts.md)

## Governing policies

- <a id="pa-afad5cab51"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-sampling-contract-lib:a_stove0_media_sampling_contract_lib](../../../evidence/sources/authorities.md#src-68256911b9) — [some-implementations/stove0/observers/contracts/media-sampling/src/a\_stove0\_media\_sampling\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-sampling/src/a_stove0_media_sampling_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_sampling_contract_lib.MediaSamplingFacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e6eaf4206c1396ba6e556187167958f37544f3f6fc372ae6ef2ef47225b191df -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "MediaSamplingArtifactFacts": {
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
        "artifacts": {
          "items": {
            "$ref": "#/$defs/MediaSamplingArtifactFacts"
          },
          "minItems": 1,
          "type": "array"
        }
      },
      "required": [
        "artifacts"
      ],
      "type": "object"
    },
    "signature": "'(*, artifacts: Annotated[tuple[a_stove0_media_sampling_contract_lib.contracts.MediaSamplingArtifactFacts, ...], MinLen(min_length=1)]) -> None'"
  },
  "distribution": "a-stove0-media-sampling-contract-lib",
  "module": "a_stove0_media_sampling_contract_lib",
  "name": "MediaSamplingFacts",
  "unit": "export"
}
```

</details>
