# stove0_review_sampler_protocol.SamplerInput

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerinput:0b92657807 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-13197b5a10"></a>
- <a id="s-5561a4b593"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-a7923095b0"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-3cc0349cf7"></a>`name`: `SamplerInput`
- <a id="s-faefb8d72f"></a>`unit`: `export`

### Declared structure

- <a id="s-c9e7fca662"></a>`kind`: `"class"`
- <a id="s-cb33753fb4"></a>`signature`: `"\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], media_type: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=255)] = None) -> None\""`

#### Validated model schema

<a id="s-0e1d0501f0"></a>
- <a id="s-9b948be3aa"></a>`title`: SamplerInput
- <a id="s-737e8e756f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5da92ff852"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-8249ce817d"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-3c43d0947e"></a>`media_type` | no | anyOf=type="string"; minLength=1; maxLength=255 \| type="null" |  |
| <a id="s-bc4133d8af"></a>`path` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-a310cd7eba"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_protocol.SamplerInput.canonical_path](stove0-review-sampler-protocol-samplerinput-canonical-path.md)

## Governing policies

- <a id="pa-093794773c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerInput`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1a38f3910cc1148ca2e3174bdfa520c9cb5cd0ab9959a8d10abc30239ae25852 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 0,
          "title": "Bytes",
          "type": "integer"
        },
        "id": {
          "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "media_type": {
          "anyOf": [
            {
              "maxLength": 255,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Media Type"
        },
        "path": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Path",
          "type": "string"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "id",
        "path",
        "bytes",
        "sha256"
      ],
      "title": "SamplerInput",
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], media_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None) -> None\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerInput",
  "unit": "export"
}
```
