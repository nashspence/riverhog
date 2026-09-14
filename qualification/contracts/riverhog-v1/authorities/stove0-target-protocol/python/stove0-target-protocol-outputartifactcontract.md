# stove0_target_protocol.OutputArtifactContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-outputartifactcontract:8196403d36 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0ce80a8e84"></a>
- <a id="s-5250210799"></a>`distribution`: `stove0-target-protocol`
- <a id="s-a6767449e9"></a>`module`: `stove0_target_protocol`
- <a id="s-2f67eb294a"></a>`name`: `OutputArtifactContract`
- <a id="s-cb47db24a9"></a>`unit`: `export`

### Declared structure

- <a id="s-bb054f5af9"></a>`kind`: `"class"`
- <a id="s-d2618ede10"></a>`signature`: `"\"(*, role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], minimum: Annotated[int, Ge(ge=0)] = 1, maximum: Annotated[int \| None, Ge(ge=1)] = None, derived_from_roles: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)]) -> None\""`

#### Validated model schema

<a id="s-613d6fa157"></a>
- <a id="s-9280c48791"></a>`title`: OutputArtifactContract
- <a id="s-37910cc5d5"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cadea0083f"></a>`derived_from_roles` | yes | type="array"; minItems=1; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-8550465219"></a>`maximum` | no | anyOf=type="integer"; minimum=1 \| type="null" |  |
| <a id="s-bc512a922a"></a>`minimum` | no | type="integer"; minimum=0 |  |
| <a id="s-f2d5ff28dd"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.OutputArtifactContract.unique_roles](stove0-target-protocol-outputartifactcontract-unique-roles.md)
- [stove0_target_protocol.OutputArtifactContract.validate_cardinality](stove0-target-protocol-outputartifactcontract-validate-cardinality.md)

## Governing policies

- <a id="pa-e48854c526"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.OutputArtifactContract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3d4e2a746fb31938f8d88fbfa6074d284f6cc8a4865db13c8369754349779b45 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "derived_from_roles": {
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "minItems": 1,
          "title": "Derived From Roles",
          "type": "array"
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
        "role",
        "derived_from_roles"
      ],
      "title": "OutputArtifactContract",
      "type": "object"
    },
    "signature": "\"(*, role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], minimum: Annotated[int, Ge(ge=0)] = 1, maximum: Annotated[int | None, Ge(ge=1)] = None, derived_from_roles: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "OutputArtifactContract",
  "unit": "export"
}
```
