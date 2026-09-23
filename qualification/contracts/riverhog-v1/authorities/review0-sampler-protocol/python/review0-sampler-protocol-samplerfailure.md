# review0_sampler_protocol.SamplerFailure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerfailure:ab78f6343e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-23015d506a"></a>
- <a id="s-baf27ae814"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-8bd89e9864"></a>`module`: `review0_sampler_protocol`
- <a id="s-7504de1310"></a>`name`: `SamplerFailure`
- <a id="s-606317af2a"></a>`unit`: `export`

### Declared structure

- <a id="s-7703da06fd"></a>`kind`: `"class"`
- <a id="s-558d98ab45"></a>`signature`: `"\"(*, code: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None\""`

#### Validated model schema

<a id="s-4eb1c7ecbe"></a>

- <a id="s-e78284d9d9"></a>`type`: `"object"`
- <a id="s-ce0d9dbdf7"></a>`additionalProperties`: `false`
- <a id="s-82bf51a111"></a>`required`: `["code","message","retryable"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5fb395af36"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e99ecee033"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-b36cddc8a5"></a>`retryable` | yes | type="boolean" |  |

## Governing policies

- <a id="pa-94716a9421"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerFailure`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 50d8db489c0cf850ac2ef4d1371b32466aa86fdec674d30fb7ff7c41651fa49a -->

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
        },
        "retryable": {
          "type": "boolean"
        }
      },
      "required": [
        "code",
        "message",
        "retryable"
      ],
      "type": "object"
    },
    "signature": "\"(*, code: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "SamplerFailure",
  "unit": "export"
}
```

</details>
