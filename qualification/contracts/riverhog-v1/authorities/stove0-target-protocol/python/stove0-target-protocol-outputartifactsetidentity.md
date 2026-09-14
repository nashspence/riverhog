# stove0_target_protocol.OutputArtifactSetIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-outputartifactsetidentity:471701b4b9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2abc66a0f6"></a>
- <a id="s-8c70c70601"></a>`distribution`: `stove0-target-protocol`
- <a id="s-c2b2a0df19"></a>`module`: `stove0_target_protocol`
- <a id="s-095e1a5956"></a>`name`: `OutputArtifactSetIdentity`
- <a id="s-d0a43c4ac2"></a>`unit`: `export`

### Declared structure

- <a id="s-abac619cb1"></a>`kind`: `"class"`
- <a id="s-62cac3dd8e"></a>`signature`: `"\"(*, artifact_count: Annotated[int, Ge(ge=1)], total_bytes: Annotated[int, Ge(ge=0)], roles: Annotated[tuple[stove0_target_protocol.protocol.OutputArtifactRoleCount, ...], MinLen(min_length=1)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-c8e742d9c0"></a>
- <a id="s-96f0d14a68"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7e7b5a02d8"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-a7e23628d4"></a>`roles` | yes | type="array"; minItems=1; items=(#/$defs/OutputArtifactRoleCount) |  |
| <a id="s-bc1d77585e"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a0c27ad3b5"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-96c477b04f"></a>`OutputArtifactRoleCount` | type="object"; fields=`count`, `role`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.OutputArtifactSetIdentity.validate_summary](stove0-target-protocol-outputartifactsetidentity-validate-summary.md)
- [stove0_target_protocol.OutputArtifactSetIdentity.seal](stove0-target-protocol-outputartifactsetidentity-seal.md)
- [stove0_target_protocol.OutputArtifactSetIdentity.seal_iterable](stove0-target-protocol-outputartifactsetidentity-seal-iterable.md)

## Governing policies

- <a id="pa-7fcd66579e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.OutputArtifactSetIdentity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 77d3a4a1f13442259afac19e5e46a0a226d9008c44830f0c18e745bd9ae8417f -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "OutputArtifactRoleCount": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "minimum": 1,
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "role",
            "count"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "artifact_count": {
          "minimum": 1,
          "type": "integer"
        },
        "roles": {
          "items": {
            "$ref": "#/$defs/OutputArtifactRoleCount"
          },
          "minItems": 1,
          "type": "array"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "total_bytes": {
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "artifact_count",
        "total_bytes",
        "roles",
        "sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, artifact_count: Annotated[int, Ge(ge=1)], total_bytes: Annotated[int, Ge(ge=0)], roles: Annotated[tuple[stove0_target_protocol.protocol.OutputArtifactRoleCount, ...], MinLen(min_length=1)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "OutputArtifactSetIdentity",
  "unit": "export"
}
```
