# stove0_media_sampling_observer_contracts.MediaSamplingFacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-sampling-observer-contracts:stove0-media-sampling-observer-contracts-265fa88d55:a8595ad000 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-sampling-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-85e1d1eea3"></a>
- <a id="s-dc30fb5161"></a>`distribution`: `stove0-media-sampling-observer-contracts`
- <a id="s-cfacc32298"></a>`module`: `stove0_media_sampling_observer_contracts`
- <a id="s-eadc555b52"></a>`name`: `MediaSamplingFacts`
- <a id="s-0474688481"></a>`unit`: `export`

### Declared structure

- <a id="s-fbb641cb60"></a>`kind`: `"class"`
- <a id="s-833ad1af84"></a>`signature`: `"'(*, artifacts: Annotated[tuple[stove0_media_sampling_observer_contracts.contracts.MediaSamplingArtifactFacts, ...], MinLen(min_length=1)]) -> None'"`

#### Validated model schema

<a id="s-7f78559b40"></a>

- <a id="s-bceebe10e7"></a>`type`: `"object"`
- <a id="s-ffeb231c84"></a>`additionalProperties`: `false`
- <a id="s-93bf105478"></a>`required`: `["artifacts"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-71eae395d0"></a>`artifacts` | yes | type="array"; items=([MediaSamplingArtifactFacts](#s-57a6c63181)); minItems=1 |  |

##### Definitions

- [MediaSamplingArtifactFacts](#s-57a6c63181)
- [SampleableRange](#s-27ea6724f4)

##### <a id="s-57a6c63181"></a>definition `MediaSamplingArtifactFacts`

- <a id="s-3d171d2a83"></a>`type`: `"object"`
- <a id="s-682aa25c0a"></a>`additionalProperties`: `false`
- <a id="s-edbbb6af09"></a>`required`: `["artifact_id","duration_ms","sampleable_ranges"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3433a1df5d"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-36a1ef84a6"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-c993dc453e"></a>`sampleable_ranges` | yes | type="array"; items=([SampleableRange](#s-27ea6724f4)); minItems=1 |  |

##### <a id="s-27ea6724f4"></a>definition `SampleableRange`

- <a id="s-77f41b48aa"></a>`type`: `"object"`
- <a id="s-07510d5a2b"></a>`additionalProperties`: `false`
- <a id="s-6f3312b776"></a>`required`: `["start_ms","duration_ms"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5f14e32aed"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-ab52779446"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Related interface records

- [canonical_artifacts](stove0-media-sampling-observer-contracts-mediasamplingfacts-canonical-artifacts.md)

## Governing policies

- <a id="pa-bd7dbcbe06"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-sampling-observer-contracts:stove0_media_sampling_observer_contracts](../../../evidence/sources.md#src-b9344d06d7) — `reference/stove0/observers/contracts/media-sampling/src/stove0_media_sampling_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_sampling_observer_contracts.MediaSamplingFacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aab2731b1e1a0fd33f0b0d6e1c328313be15442354d877cbdb449fcbcf801ebb -->

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
    "signature": "'(*, artifacts: Annotated[tuple[stove0_media_sampling_observer_contracts.contracts.MediaSamplingArtifactFacts, ...], MinLen(min_length=1)]) -> None'"
  },
  "distribution": "stove0-media-sampling-observer-contracts",
  "module": "stove0_media_sampling_observer_contracts",
  "name": "MediaSamplingFacts",
  "unit": "export"
}
```

</details>
