# stove0_protocol.CoordinationCollectionResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-coordinationcollectionresult:f9cfb844c5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fa965f5bc0"></a>
- <a id="s-ed71943913"></a>`distribution`: `stove0-protocol`
- <a id="s-98d8031b1c"></a>`module`: `stove0_protocol`
- <a id="s-bfc2a73cdc"></a>`name`: `CoordinationCollectionResult`
- <a id="s-9465c59ef7"></a>`unit`: `export`

### Declared structure

- <a id="s-f118c7617e"></a>`kind`: `"class"`
- <a id="s-167d14ca6b"></a>`signature`: `"\"(*, producer_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_protocol.models.CollectionRootRef, output_selection: stove0_protocol.fork_join.ArtifactSelectionRef) -> None\""`

#### Validated model schema

<a id="s-34b10d7254"></a>
- <a id="s-11c104ce99"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cc9f456d3c"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c86826f870"></a>`join_settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2b815340a1"></a>`output_collection` | yes | #/$defs/CollectionRootRef |  |
| <a id="s-03cda085c4"></a>`output_selection` | yes | #/$defs/ArtifactSelectionRef |  |
| <a id="s-5848a46fac"></a>`producer_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-1a5436700c"></a>`ArtifactSelectionRef` | type="object"; fields=`artifact_count`, `selection_sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-1343125861"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-a8c5328325"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-93d882f42f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.CoordinationCollectionResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1811a832461226c2583e982c0d97c1cb77d80a6d9c6eedf55c7323158f4a2877 -->

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
        "derivation_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "join_settlement_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "output_collection": {
          "$ref": "#/$defs/CollectionRootRef"
        },
        "output_selection": {
          "$ref": "#/$defs/ArtifactSelectionRef"
        },
        "producer_work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "producer_work_id",
        "join_settlement_sha256",
        "derivation_sha256",
        "output_collection",
        "output_selection"
      ],
      "type": "object"
    },
    "signature": "\"(*, producer_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_protocol.models.CollectionRootRef, output_selection: stove0_protocol.fork_join.ArtifactSelectionRef) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "CoordinationCollectionResult",
  "unit": "export"
}
```
