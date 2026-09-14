# stove0_target_support.OutputArtifactContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-outputartifactcontract:b3db05b34b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-272d53846e"></a>
- <a id="s-bb45048b43"></a>`distribution`: `stove0-target-support`
- <a id="s-b4b0a9aa77"></a>`module`: `stove0_target_support`
- <a id="s-9a1264036c"></a>`name`: `OutputArtifactContract`
- <a id="s-8f23b1dc99"></a>`unit`: `export`

### Declared structure

- <a id="s-8fa947c58c"></a>`kind`: `"class"`
- <a id="s-469214a1e9"></a>`signature`: `"\"(*, role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], minimum: Annotated[int, Ge(ge=0)] = 1, maximum: Annotated[int \| None, Ge(ge=1)] = None, derived_from_roles: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)]) -> None\""`

#### Validated model schema

<a id="s-37bafc7f25"></a>
- <a id="s-f8722b9541"></a>`title`: OutputArtifactContract
- <a id="s-fc94cff2cc"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a481c53b16"></a>`derived_from_roles` | yes | type="array"; minItems=1; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-d8883937ed"></a>`maximum` | no | anyOf=type="integer"; minimum=1 \| type="null" |  |
| <a id="s-a7fa2a9e89"></a>`minimum` | no | type="integer"; minimum=0 |  |
| <a id="s-6404d08988"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [stove0_target_support.OutputArtifactContract.unique_roles](stove0-target-support-outputartifactcontract-unique-roles.md)
- [stove0_target_support.OutputArtifactContract.validate_cardinality](stove0-target-support-outputartifactcontract-validate-cardinality.md)

## Governing policies

- <a id="pa-885af9e524"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.OutputArtifactContract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1792a3b2a2aef1154f3a5cbc2641e5244200914521e2df2d85feda281111e7a8 -->

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
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "OutputArtifactContract",
  "unit": "export"
}
```
