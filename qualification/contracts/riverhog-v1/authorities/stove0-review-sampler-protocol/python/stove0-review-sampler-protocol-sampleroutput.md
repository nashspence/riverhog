# stove0_review_sampler_protocol.SamplerOutput

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-sampleroutput:fdddc22526 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-68bc0bed73"></a>
- <a id="s-2539bfcd6a"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-814ef07be9"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-23416d7d34"></a>`name`: `SamplerOutput`
- <a id="s-cbc124711e"></a>`unit`: `export`

### Declared structure

- <a id="s-c7a45ce33c"></a>`kind`: `"class"`
- <a id="s-d423acb22b"></a>`signature`: `"\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], media_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], derived_from: Annotated[tuple[str, ...], MinLen(min_length=1)]) -> None\""`

#### Validated model schema

<a id="s-d01a821efc"></a>

- <a id="s-ff5d956d3d"></a>`type`: `"object"`
- <a id="s-6cbe358550"></a>`additionalProperties`: `false`
- <a id="s-e5c84ee0b2"></a>`required`: `["id","path","bytes","sha256","media_type","derived_from"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-241ceaa52d"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-ff7d077529"></a>`derived_from` | yes | type="array"; items=(type="string"); minItems=1 |  |
| <a id="s-35b95467de"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-57436e5d43"></a>`media_type` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-d3292fc5f1"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-ba93cf995a"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [canonical_sources](stove0-review-sampler-protocol-sampleroutput-canonical-sources.md)
- [canonical_path](stove0-review-sampler-protocol-sampleroutput-canonical-path.md)

## Governing policies

- <a id="pa-21da62be9c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources/authorities.md#src-25c43eb779) — [reference/stove0/targets/review/sampler/protocol/src/stove0\_review\_sampler\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerOutput`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bf8ca0efbeed3b4b6984aa48a77563a0ddb55afff33611f2d32b6dcb2e9367c1 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "derived_from": {
          "items": {
            "type": "string"
          },
          "minItems": 1,
          "type": "array"
        },
        "id": {
          "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
          "type": "string"
        },
        "media_type": {
          "maxLength": 255,
          "minLength": 1,
          "type": "string"
        },
        "path": {
          "maxLength": 4096,
          "minLength": 1,
          "type": "string"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "id",
        "path",
        "bytes",
        "sha256",
        "media_type",
        "derived_from"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], media_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], derived_from: Annotated[tuple[str, ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerOutput",
  "unit": "export"
}
```

</details>
