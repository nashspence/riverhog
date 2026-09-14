# stove0_target_protocol.InputArtifactContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-inputartifactcontract:0409e67fd7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bf6f1ab052"></a>
- <a id="s-ba1d3e93a1"></a>`distribution`: `stove0-target-protocol`
- <a id="s-deae47df1f"></a>`module`: `stove0_target_protocol`
- <a id="s-a8fc3566c9"></a>`name`: `InputArtifactContract`
- <a id="s-e937d8cf47"></a>`unit`: `export`

### Declared structure

- <a id="s-c05bbe6aa1"></a>`kind`: `"class"`
- <a id="s-768712fd8a"></a>`signature`: `"\"(*, role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], minimum: Annotated[int, Ge(ge=0)] = 1, maximum: Annotated[int \| None, Ge(ge=1)] = None, allowed_dispositions: tuple[typing.Literal['transformed', 'preserved', 'omitted', 'rejected'], ...] \| None = None) -> None\""`

#### Validated model schema

<a id="s-e2ea8b8c55"></a>
- <a id="s-3b95ea2a4b"></a>`title`: InputArtifactContract
- <a id="s-1f0e29ee03"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a561d92e4d"></a>`allowed_dispositions` | no | anyOf=type="array"; items=(type="string"; enum=["transformed","preserved","omitted","rejected"]) \| type="null" |  |
| <a id="s-a3c8a665ac"></a>`maximum` | no | anyOf=type="integer"; minimum=1 \| type="null" |  |
| <a id="s-00cc278bb3"></a>`minimum` | no | type="integer"; minimum=0 |  |
| <a id="s-9852d50fa4"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.InputArtifactContract.validate_cardinality](stove0-target-protocol-inputartifactcontract-validate-cardinality.md)
- [stove0_target_protocol.InputArtifactContract.canonical_dispositions](stove0-target-protocol-inputartifactcontract-canonical-dispositions.md)

## Governing policies

- <a id="pa-4a52ea6690"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.InputArtifactContract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9632ded51d38cef96298c13d36dd3640665927a0d0ab7aab9dad47e394eabce4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "allowed_dispositions": {
          "anyOf": [
            {
              "items": {
                "enum": [
                  "transformed",
                  "preserved",
                  "omitted",
                  "rejected"
                ],
                "type": "string"
              },
              "type": "array"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Allowed Dispositions"
        },
        "maximum": {
          "anyOf": [
            {
              "minimum": 1,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Maximum"
        },
        "minimum": {
          "default": 1,
          "minimum": 0,
          "title": "Minimum",
          "type": "integer"
        },
        "role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Role",
          "type": "string"
        }
      },
      "required": [
        "role"
      ],
      "title": "InputArtifactContract",
      "type": "object"
    },
    "signature": "\"(*, role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], minimum: Annotated[int, Ge(ge=0)] = 1, maximum: Annotated[int | None, Ge(ge=1)] = None, allowed_dispositions: tuple[typing.Literal['transformed', 'preserved', 'omitted', 'rejected'], ...] | None = None) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "InputArtifactContract",
  "unit": "export"
}
```
