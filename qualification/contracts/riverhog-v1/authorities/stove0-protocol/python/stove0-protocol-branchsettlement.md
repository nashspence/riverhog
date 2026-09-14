# stove0_protocol.BranchSettlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsettlement:29cad94916 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-caa0797d38"></a>
- <a id="s-4ce63a798f"></a>`distribution`: `stove0-protocol`
- <a id="s-49a5e90287"></a>`module`: `stove0_protocol`
- <a id="s-c3ced75411"></a>`name`: `BranchSettlement`
- <a id="s-36f8a832ac"></a>`unit`: `export`

### Declared structure

- <a id="s-64b76d5bed"></a>`kind`: `"class"`
- <a id="s-8045641d7e"></a>`signature`: `"\"(*, format: Literal['stove0-branch-settlement/v1'] = 'stove0-branch-settlement/v1', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], producer_settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_protocol.models.CollectionRootRef, output_selection: stove0_protocol.fork_join.ArtifactSelectionRef, settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-c16e390dee"></a>
- <a id="s-8e5090f602"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-71fbbd5387"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-770e80c69c"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-46e2d6daa9"></a>`format` | no | type="string"; const="stove0-branch-settlement/v1" |  |
| <a id="s-b9c57d8b77"></a>`output_collection` | yes | #/$defs/CollectionRootRef |  |
| <a id="s-cd75d4fa1f"></a>`output_selection` | yes | #/$defs/ArtifactSelectionRef |  |
| <a id="s-4df396d120"></a>`producer_settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3f4a3ac115"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-41d92abd43"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a8cea05120"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-ce9acaea80"></a>`ArtifactSelectionRef` | type="object"; fields=`artifact_count`, `selection_sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-80fd89a6d5"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-0afcd6e1e3"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [stove0_protocol.BranchSettlement.seal](stove0-protocol-branchsettlement-seal.md)
- [stove0_protocol.BranchSettlement.verify_digest](stove0-protocol-branchsettlement-verify-digest.md)

## Governing policies

- <a id="pa-92b2d5278a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSettlement`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eae72e1efdbc0c7741f3b79f1a7d04a4ee917dbc01d1a51269ee5e362ac6e7e9 -->

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
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        },
        "CollectionRootRef": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "branch_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "derivation_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "format": {
          "const": "stove0-branch-settlement/v1",
          "default": "stove0-branch-settlement/v1",
          "type": "string"
        },
        "output_collection": {
          "$ref": "#/$defs/CollectionRootRef"
        },
        "output_selection": {
          "$ref": "#/$defs/ArtifactSelectionRef"
        },
        "producer_settlement_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "settlement_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "workflow_plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "branch_id",
        "work_id",
        "workflow_plan_sha256",
        "derivation_sha256",
        "producer_settlement_sha256",
        "output_collection",
        "output_selection",
        "settlement_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-branch-settlement/v1'] = 'stove0-branch-settlement/v1', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], producer_settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_protocol.models.CollectionRootRef, output_selection: stove0_protocol.fork_join.ArtifactSelectionRef, settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BranchSettlement",
  "unit": "export"
}
```
