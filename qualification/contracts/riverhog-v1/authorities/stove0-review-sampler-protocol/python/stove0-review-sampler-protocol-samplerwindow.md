# stove0_review_sampler_protocol.SamplerWindow

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerwindow:2500c294f3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f0d20e20a8"></a>
- <a id="s-e21618d727"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-effc939627"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-e476694ee5"></a>`name`: `SamplerWindow`
- <a id="s-933428f98e"></a>`unit`: `export`

### Declared structure

- <a id="s-c8ca04aae2"></a>`kind`: `"class"`
- <a id="s-d18e471956"></a>`signature`: `"\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], input_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], start_ms: Annotated[int, Ge(ge=0)], duration_ms: Annotated[int, Ge(ge=1)], output_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)]) -> None\""`

#### Validated model schema

<a id="s-f3de5ccbcb"></a>
- <a id="s-2c05f7dc67"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0690e04aad"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-57e429dc7b"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-16acf6a5d0"></a>`input_id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-99677eca01"></a>`output_path` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-99eed2264c"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Related interface records

- [stove0_review_sampler_protocol.SamplerWindow.canonical_output_path](stove0-review-sampler-protocol-samplerwindow-canonical-output-path.md)

## Governing policies

- <a id="pa-cc1e4ecb10"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerWindow`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 66152fd7c8a5a0f17ef78cc51fca7daae1bac2c03e69732cc0217fd0a7b9b935 -->

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
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "SamplerWindow",
  "unit": "export"
}
```
