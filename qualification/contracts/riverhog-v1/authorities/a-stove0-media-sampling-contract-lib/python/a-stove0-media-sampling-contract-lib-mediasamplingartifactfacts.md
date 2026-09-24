# a_stove0_media_sampling_contract_lib.MediaSamplingArtifactFacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-sampling-contract-lib:a-stove0-media-sampling-contract-lib-medi-4e9eb59e74:3f86e50ff4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-sampling-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8285fb536c"></a>
- <a id="s-a9a1a9cf53"></a>`distribution`: `a-stove0-media-sampling-contract-lib`
- <a id="s-512efb8837"></a>`module`: `a_stove0_media_sampling_contract_lib`
- <a id="s-6ebb0f0976"></a>`name`: `MediaSamplingArtifactFacts`
- <a id="s-50ef43b6cb"></a>`unit`: `export`

### Declared structure

- <a id="s-dbf46434f9"></a>`kind`: `"class"`
- <a id="s-5589f9699d"></a>`signature`: `"'(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], duration_ms: Annotated[int, Ge(ge=1)], sampleable_ranges: Annotated[tuple[a_stove0_media_sampling_contract_lib.contracts.SampleableRange, ...], MinLen(min_length=1)]) -> None'"`

#### Validated model schema

<a id="s-eeeccb8cb7"></a>

- <a id="s-cc5f8f6723"></a>`type`: `"object"`
- <a id="s-4e0f8e3c65"></a>`additionalProperties`: `false`
- <a id="s-b61c5668a5"></a>`required`: `["artifact_id","duration_ms","sampleable_ranges"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c613aa36d1"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-9d9aab936f"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-572f271913"></a>`sampleable_ranges` | yes | type="array"; items=([SampleableRange](#s-929e6a672f)); minItems=1 |  |

##### Definitions

- [SampleableRange](#s-929e6a672f)

##### <a id="s-929e6a672f"></a>definition `SampleableRange`

- <a id="s-0266d6cf92"></a>`type`: `"object"`
- <a id="s-b3002d4562"></a>`additionalProperties`: `false`
- <a id="s-6dede0291e"></a>`required`: `["start_ms","duration_ms"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2246d8af27"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-5c2a8c25e0"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Related interface records

- [validate_ranges](a-stove0-media-sampling-contract-lib-mediasamplingartifactfacts-validate-ranges.md)

## Governing policies

- <a id="pa-b897294719"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-sampling-contract-lib:a_stove0_media_sampling_contract_lib](../../../evidence/sources/authorities.md#src-68256911b9) — [some-implementations/stove0/observers/contracts/media-sampling/src/a\_stove0\_media\_sampling\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-sampling/src/a_stove0_media_sampling_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_sampling_contract_lib.MediaSamplingArtifactFacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 97338524176c304fb7b554e4505739cd0b9a220b8fc83233265ff50112463d47 -->

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
    "signature": "'(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], duration_ms: Annotated[int, Ge(ge=1)], sampleable_ranges: Annotated[tuple[a_stove0_media_sampling_contract_lib.contracts.SampleableRange, ...], MinLen(min_length=1)]) -> None'"
  },
  "distribution": "a-stove0-media-sampling-contract-lib",
  "module": "a_stove0_media_sampling_contract_lib",
  "name": "MediaSamplingArtifactFacts",
  "unit": "export"
}
```

</details>
