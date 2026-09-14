# stove0_target_protocol.TargetInputAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetinputauthority:711241aca6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-067371687d"></a>
- <a id="s-accd9dad7a"></a>`distribution`: `stove0-target-protocol`
- <a id="s-3bec17ad59"></a>`module`: `stove0_target_protocol`
- <a id="s-087ddfad32"></a>`name`: `TargetInputAuthority`
- <a id="s-ade6ef5aa1"></a>`unit`: `export`

### Declared structure

- <a id="s-81fd453c30"></a>`kind`: `"class"`
- <a id="s-53902bb0b0"></a>`signature`: `"'(*, selection: stove0_protocol.fork_join.ArtifactSelectionRef, roles: Annotated[tuple[stove0_target_protocol.protocol.TargetInputRoleCount, ...], MinLen(min_length=1)]) -> None'"`

#### Validated model schema

<a id="s-6bccd1f0d0"></a>
- <a id="s-07727ea055"></a>`title`: TargetInputAuthority
- <a id="s-f8fa8f0988"></a>`description`: Small exact input authority retained by Stove0 and traversed in bounded pages.
- <a id="s-748fbaccba"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c0a9935e5a"></a>`roles` | yes | type="array"; minItems=1; items=(#/$defs/TargetInputRoleCount) |  |
| <a id="s-096ed0cfbb"></a>`selection` | yes | #/$defs/ArtifactSelectionRef |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-7dc6a4ef18"></a>`ArtifactSelectionRef` | type="object"; fields=`artifact_count`, `selection_sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-08566a1ea6"></a>`TargetInputRoleCount` | type="object"; fields=`count`, `role`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetInputAuthority.from_selection](stove0-target-protocol-targetinputauthority-from-selection.md)
- [stove0_target_protocol.TargetInputAuthority.validate_summary](stove0-target-protocol-targetinputauthority-validate-summary.md)

## Governing policies

- <a id="pa-90727362db"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetInputAuthority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1326bd288bf4405c010c53c7de90b80aadff4361d215d292f60e568c3e64e61b -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactSelectionRef": {
          "additionalProperties": false,
          "description": "Closed reference to a separately retained selection document.",
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "title": "Artifact Count",
              "type": "integer"
            },
            "selection_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Selection Sha256",
              "type": "string"
            },
            "total_bytes": {
              "minimum": 0,
              "title": "Total Bytes",
              "type": "integer"
            }
          },
          "required": [
            "selection_sha256",
            "artifact_count",
            "total_bytes"
          ],
          "title": "ArtifactSelectionRef",
          "type": "object"
        },
        "TargetInputRoleCount": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "minimum": 1,
              "title": "Count",
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
            "count"
          ],
          "title": "TargetInputRoleCount",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "description": "Small exact input authority retained by Stove0 and traversed in bounded pages.",
      "properties": {
        "roles": {
          "items": {
            "$ref": "#/$defs/TargetInputRoleCount"
          },
          "minItems": 1,
          "title": "Roles",
          "type": "array"
        },
        "selection": {
          "$ref": "#/$defs/ArtifactSelectionRef"
        }
      },
      "required": [
        "selection",
        "roles"
      ],
      "title": "TargetInputAuthority",
      "type": "object"
    },
    "signature": "'(*, selection: stove0_protocol.fork_join.ArtifactSelectionRef, roles: Annotated[tuple[stove0_target_protocol.protocol.TargetInputRoleCount, ...], MinLen(min_length=1)]) -> None'"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetInputAuthority",
  "unit": "export"
}
```
