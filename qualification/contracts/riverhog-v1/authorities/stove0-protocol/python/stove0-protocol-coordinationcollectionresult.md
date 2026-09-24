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
- <a id="s-167d14ca6b"></a>`signature`: `"\"(*, producer_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_protocol.models.CollectionRootIdentityRef, output_selection: stove0_protocol.fork_join.ArtifactSelectionRef) -> None\""`

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
| <a id="s-2b815340a1"></a>`output_collection` | yes | [CollectionRootIdentityRef](#s-fb0871d52b) |  |
| <a id="s-03cda085c4"></a>`output_selection` | yes | [ArtifactSelectionRef](#s-1a5436700c) |  |
| <a id="s-5848a46fac"></a>`producer_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [ArtifactSelectionRef](#s-1a5436700c)
- [CollectionId](#s-1343125861)
- [CollectionRootIdentityRef](#s-fb0871d52b)
- [NonnegativeDecimal](#s-866b5b0fb0)

##### <a id="s-1a5436700c"></a>definition `ArtifactSelectionRef`

- <a id="s-b76bdc6384"></a>`type`: `"object"`
- <a id="s-cd573bef5a"></a>`additionalProperties`: `false`
- <a id="s-09c8577b8e"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e80c82f7a3"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-2101ecb9cb"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0c9a3cb892"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-866b5b0fb0); ge=0 |  |

##### <a id="s-1343125861"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-4342aeb836"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-37dde26e1d"></a>2 | not=(const="0") |

##### <a id="s-fb0871d52b"></a>definition `CollectionRootIdentityRef`

- <a id="s-9c655614c0"></a>`type`: `"object"`
- <a id="s-f2eff3087e"></a>`additionalProperties`: `false`
- <a id="s-f0d3571540"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a4a75c0b74"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-04de13fad8"></a>`collection_id` | yes | [CollectionId](#s-1343125861) |  |
| <a id="s-2d5727947d"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-866b5b0fb0"></a>definition `NonnegativeDecimal`

- <a id="s-ede592f8f0"></a>`type`: `"string"`
- <a id="s-ecc361aac9"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Governing policies

- <a id="pa-93d882f42f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.CoordinationCollectionResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 980d25f38456e9c4e5d4ebf9e42df2f530ad34577baa8b09864fb90fbf8e6682 -->

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
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 0
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
          "allOf": [
            {
              "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
              "type": "string"
            },
            {
              "not": {
                "const": "0"
              }
            }
          ]
        },
        "CollectionRootIdentityRef": {
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
        },
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
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
          "$ref": "#/$defs/CollectionRootIdentityRef"
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
    "signature": "\"(*, producer_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_protocol.models.CollectionRootIdentityRef, output_selection: stove0_protocol.fork_join.ArtifactSelectionRef) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "CoordinationCollectionResult",
  "unit": "export"
}
```

</details>
