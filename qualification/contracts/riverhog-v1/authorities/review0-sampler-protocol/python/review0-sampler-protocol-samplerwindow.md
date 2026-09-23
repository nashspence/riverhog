# review0_sampler_protocol.SamplerWindow

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerwindow:927dd469f0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3f50128623"></a>
- <a id="s-9d4b3ca75a"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-3f524c3278"></a>`module`: `review0_sampler_protocol`
- <a id="s-9ee58235da"></a>`name`: `SamplerWindow`
- <a id="s-a69498a671"></a>`unit`: `export`

### Declared structure

- <a id="s-68152fb7a4"></a>`kind`: `"class"`
- <a id="s-fbcfe8bcaf"></a>`signature`: `"\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], input_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], start_ms: Annotated[int, Ge(ge=0)], duration_ms: Annotated[int, Ge(ge=1)], output_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)]) -> None\""`

#### Validated model schema

<a id="s-a4aab6e777"></a>

- <a id="s-3d78449ef1"></a>`type`: `"object"`
- <a id="s-f130d11b05"></a>`additionalProperties`: `false`
- <a id="s-a4c3cb43c8"></a>`required`: `["id","input_id","start_ms","duration_ms","output_path"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4789f3b289"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-36f5e8ec6a"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-4e90a8fb8e"></a>`input_id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-b76c39564b"></a>`output_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-f3a0daa00e"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Related interface records

- [canonical_output_path](review0-sampler-protocol-samplerwindow-canonical-output-path.md)

## Governing policies

- <a id="pa-c4f990d62e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerWindow`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 72fd21ce1ad1eaae99506cc104f75cec158981b22916f55f57090e8726ee674e -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "duration_ms": {
          "minimum": 1,
          "type": "integer"
        },
        "id": {
          "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
          "type": "string"
        },
        "input_id": {
          "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
          "type": "string"
        },
        "output_path": {
          "maxLength": 4096,
          "minLength": 1,
          "type": "string"
        },
        "start_ms": {
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "id",
        "input_id",
        "start_ms",
        "duration_ms",
        "output_path"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], input_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], start_ms: Annotated[int, Ge(ge=0)], duration_ms: Annotated[int, Ge(ge=1)], output_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)]) -> None\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "SamplerWindow",
  "unit": "export"
}
```

</details>
