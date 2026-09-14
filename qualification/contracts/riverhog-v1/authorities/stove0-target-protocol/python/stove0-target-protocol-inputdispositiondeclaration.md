# stove0_target_protocol.InputDispositionDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-inputdispositiondeclaration:7a2971473a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-344e90d3fe"></a>
- <a id="s-1d61b0cc2a"></a>`distribution`: `stove0-target-protocol`
- <a id="s-7ca58080c5"></a>`module`: `stove0_target_protocol`
- <a id="s-efa10387ef"></a>`name`: `InputDispositionDeclaration`
- <a id="s-f83d7dd192"></a>`unit`: `export`

### Declared structure

- <a id="s-853307b2e5"></a>`kind`: `"class"`
- <a id="s-0c0a39e708"></a>`signature`: `"\"(*, input_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], status: Literal['transformed', 'preserved', 'omitted', 'rejected']) -> None\""`

#### Validated model schema

<a id="s-f7679a5222"></a>
- <a id="s-3cdb6938b6"></a>`title`: InputDispositionDeclaration
- <a id="s-b12ac75dd7"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7b027a690a"></a>`input_id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-4d2ccd5c47"></a>`status` | yes | type="string"; enum=["transformed","preserved","omitted","rejected"] |  |

## Governing policies

- <a id="pa-49f2dfd085"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.InputDispositionDeclaration`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 385006afdad7840ae531df0643948d640c366eaa377cd0549626a0c5fc2ac1f0 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "input_id": {
          "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
          "title": "Input Id",
          "type": "string"
        },
        "status": {
          "enum": [
            "transformed",
            "preserved",
            "omitted",
            "rejected"
          ],
          "title": "Status",
          "type": "string"
        }
      },
      "required": [
        "input_id",
        "status"
      ],
      "title": "InputDispositionDeclaration",
      "type": "object"
    },
    "signature": "\"(*, input_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], status: Literal['transformed', 'preserved', 'omitted', 'rejected']) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "InputDispositionDeclaration",
  "unit": "export"
}
```
