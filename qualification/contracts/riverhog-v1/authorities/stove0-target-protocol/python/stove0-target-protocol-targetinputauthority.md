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

- <a id="s-748fbaccba"></a>`type`: `"object"`
- <a id="s-1fe3bc7063"></a>`additionalProperties`: `false`
- <a id="s-88b77d328d"></a>`required`: `["selection","roles"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c0a9935e5a"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-08566a1ea6)); minItems=1 |  |
| <a id="s-096ed0cfbb"></a>`selection` | yes | [ArtifactSelectionRef](#s-7dc6a4ef18) |  |

##### Definitions

- [ArtifactSelectionRef](#s-7dc6a4ef18)
- [TargetInputRoleCount](#s-08566a1ea6)

##### <a id="s-7dc6a4ef18"></a>definition `ArtifactSelectionRef`

- <a id="s-c7415149f0"></a>`type`: `"object"`
- <a id="s-c5d3c42120"></a>`additionalProperties`: `false`
- <a id="s-33a2655842"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1e77610fdf"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-bc5da194c7"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a9fab63d2a"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-08566a1ea6"></a>definition `TargetInputRoleCount`

- <a id="s-46c97d9f40"></a>`type`: `"object"`
- <a id="s-f64fdb41ca"></a>`additionalProperties`: `false`
- <a id="s-f86eae8eee"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-72fc27cb60"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-317339f725"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [from_selection](stove0-target-protocol-targetinputauthority-from-selection.md)
- [validate_summary](stove0-target-protocol-targetinputauthority-validate-summary.md)

## Governing policies

- <a id="pa-90727362db"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetInputAuthority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bb35e2ac1a6cff851b887443ddd48df408598268b93467d162df61e39426df41 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactSelectionRef": {
          "additionalProperties": false,
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "type": "integer"
            },
            "selection_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "total_bytes": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "selection_sha256",
            "artifact_count",
            "total_bytes"
          ],
          "type": "object"
        },
        "TargetInputRoleCount": {
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
        "roles": {
          "items": {
            "$ref": "#/$defs/TargetInputRoleCount"
          },
          "minItems": 1,
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

</details>
