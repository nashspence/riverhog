# stove0_protocol.CoordinationCollectionResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-coordinationcollectionresult:f9cfb844c5 -->

Exact externally visible contract owned by this contract element.

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

- <a id="s-11c104ce99"></a>`type`: `"object"`
- <a id="s-dc436479a8"></a>`additionalProperties`: `false`
- <a id="s-942d392985"></a>`required`: `["producer_work_id","join_settlement_sha256","derivation_sha256","output_collection","output_selection"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cc9f456d3c"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c86826f870"></a>`join_settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2b815340a1"></a>`output_collection` | yes | [CollectionRootRef](#s-a8c5328325) |  |
| <a id="s-03cda085c4"></a>`output_selection` | yes | [ArtifactSelectionRef](#s-1a5436700c) |  |
| <a id="s-5848a46fac"></a>`producer_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [ArtifactSelectionRef](#s-1a5436700c)
- [CollectionId](#s-1343125861)
- [CollectionRootRef](#s-a8c5328325)

##### <a id="s-1a5436700c"></a>definition `ArtifactSelectionRef`

- <a id="s-b76bdc6384"></a>`type`: `"object"`
- <a id="s-cd573bef5a"></a>`additionalProperties`: `false`
- <a id="s-09c8577b8e"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e80c82f7a3"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-2101ecb9cb"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0c9a3cb892"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-1343125861"></a>definition `CollectionId`

- <a id="s-ddea686c3f"></a>`type`: `"integer"`
- <a id="s-34e27af618"></a>`minimum`: `1`

##### <a id="s-a8c5328325"></a>definition `CollectionRootRef`

- <a id="s-aefa0ea39f"></a>`type`: `"object"`
- <a id="s-54d97cbabf"></a>`additionalProperties`: `false`
- <a id="s-4dbe3fdc83"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0a4b886ae5"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-80c35894a9"></a>`collection_id` | yes | [CollectionId](#s-1343125861) |  |
| <a id="s-3d747893a4"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Governing policies

- <a id="pa-93d882f42f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.CoordinationCollectionResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
