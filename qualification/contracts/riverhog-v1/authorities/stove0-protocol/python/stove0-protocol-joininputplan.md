# stove0_protocol.JoinInputPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joininputplan:e420c38797 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8f37825ea4"></a>
- <a id="s-e642bfaa53"></a>`distribution`: `stove0-protocol`
- <a id="s-63981b696b"></a>`module`: `stove0_protocol`
- <a id="s-f5c56c9243"></a>`name`: `JoinInputPlan`
- <a id="s-d5fd69ab31"></a>`unit`: `export`

### Declared structure

- <a id="s-d9a49f23f6"></a>`kind`: `"class"`
- <a id="s-ec691f91f1"></a>`signature`: `"\"(*, branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], producer_settlement_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_protocol.models.CollectionRootRef, artifact_selection: stove0_protocol.fork_join.ArtifactSelectionRef) -> None\""`

#### Validated model schema

<a id="s-1e1eec242d"></a>
- <a id="s-7bf5ef4f08"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2628f337ae"></a>`artifact_selection` | yes | #/$defs/ArtifactSelectionRef |  |
| <a id="s-1758585307"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-399a33a868"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3a16c54223"></a>`output_collection` | yes | #/$defs/CollectionRootRef |  |
| <a id="s-c6325dd528"></a>`producer_settlement_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-1a5f4be627"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-43f70ddd0b"></a>`ArtifactSelectionRef` | type="object"; fields=`artifact_count`, `selection_sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-8b4631ef43"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-2939803369"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-0686f09a85"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JoinInputPlan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a5dfe2f0fb4859bba0c212d60a1ce500d440cce309cd0b382ca378c7d9bb1bfb -->

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
        "artifact_selection": {
          "$ref": "#/$defs/ArtifactSelectionRef"
        },
        "branch_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "derivation_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "output_collection": {
          "$ref": "#/$defs/CollectionRootRef"
        },
        "producer_settlement_sha256": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "settlement_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "branch_id",
        "settlement_sha256",
        "derivation_sha256",
        "output_collection",
        "artifact_selection"
      ],
      "type": "object"
    },
    "signature": "\"(*, branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], producer_settlement_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_protocol.models.CollectionRootRef, artifact_selection: stove0_protocol.fork_join.ArtifactSelectionRef) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JoinInputPlan",
  "unit": "export"
}
```
