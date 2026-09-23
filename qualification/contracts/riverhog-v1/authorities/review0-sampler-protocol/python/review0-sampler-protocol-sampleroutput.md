# review0_sampler_protocol.SamplerOutput

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-sampleroutput:fa30cfe816 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a7aa8a0a9d"></a>
- <a id="s-ca5e67f088"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-403ec21edf"></a>`module`: `review0_sampler_protocol`
- <a id="s-264b45783c"></a>`name`: `SamplerOutput`
- <a id="s-3073a7b2ad"></a>`unit`: `export`

### Declared structure

- <a id="s-a05226692c"></a>`kind`: `"class"`
- <a id="s-2a65edc173"></a>`signature`: `"\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], media_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], derived_from: Annotated[tuple[str, ...], MinLen(min_length=1)]) -> None\""`

#### Validated model schema

<a id="s-ca33979747"></a>

- <a id="s-89968e3a37"></a>`type`: `"object"`
- <a id="s-339e1e329c"></a>`additionalProperties`: `false`
- <a id="s-7a5cfc7b83"></a>`required`: `["id","path","bytes","sha256","media_type","derived_from"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6be7a06bff"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-f359e5e59a"></a>`derived_from` | yes | type="array"; items=(type="string"); minItems=1 |  |
| <a id="s-b9f8131625"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-69cd3fc8a8"></a>`media_type` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-ba2724cfc5"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-b184c7f49e"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [canonical_sources](review0-sampler-protocol-sampleroutput-canonical-sources.md)
- [canonical_path](review0-sampler-protocol-sampleroutput-canonical-path.md)

## Governing policies

- <a id="pa-f0d92f0021"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerOutput`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3c1cea139d91c7912bfad8a639b1d830cf1a3d9e83794795da726aa9d710030e -->

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
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "SamplerOutput",
  "unit": "export"
}
```

</details>
