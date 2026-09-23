# review0_sampler_protocol.SamplerInapplicable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerinapplicable:4347330cd3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2bd19b2450"></a>
- <a id="s-1049ba0972"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-dee5fb87b1"></a>`module`: `review0_sampler_protocol`
- <a id="s-ac682f5e74"></a>`name`: `SamplerInapplicable`
- <a id="s-1d5a3ccbe0"></a>`unit`: `export`

### Declared structure

- <a id="s-f0ccb605fb"></a>`kind`: `"class"`
- <a id="s-eeeaf81fb7"></a>`signature`: `"\"(*, code: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None\""`

#### Validated model schema

<a id="s-1796db16e1"></a>

- <a id="s-095f5328ad"></a>`type`: `"object"`
- <a id="s-2e11371168"></a>`additionalProperties`: `false`
- <a id="s-679e5b68b9"></a>`required`: `["code","message"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7b472302dd"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-3f9388bb73"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |

## Governing policies

- <a id="pa-45f830fb4a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerInapplicable`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2dbfcd460e9a00f449061cfbd29dae99f3a02e667456918ee5c6f31d6ba13d73 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "code",
        "message"
      ],
      "type": "object"
    },
    "signature": "\"(*, code: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "SamplerInapplicable",
  "unit": "export"
}
```

</details>
