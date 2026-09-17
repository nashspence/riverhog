# stove0_target_protocol.TargetSettlementAuthorityPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetsettlementau-61bbf99e56:636d04ed1b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-350d51121d"></a>
- <a id="s-cc6031559f"></a>`distribution`: `stove0-target-protocol`
- <a id="s-91700a557a"></a>`module`: `stove0_target_protocol`
- <a id="s-e6b072b23b"></a>`name`: `TargetSettlementAuthorityPayload`
- <a id="s-0e8755349b"></a>`unit`: `export`

### Declared structure

- <a id="s-1c3af33263"></a>`kind`: `"class"`
- <a id="s-268ab5b9a5"></a>`signature`: `"\"(*, format: Literal['stove0-target-settlement/v1'] = 'stove0-target-settlement/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], production_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_target_protocol.protocol.OutputCollectionRef, output_bindings: stove0_target_protocol.protocol.TargetOutputBindingSetIdentity) -> None\""`

#### Validated model schema

<a id="s-874c7f4608"></a>

- <a id="s-155bb0da2e"></a>`type`: `"object"`
- <a id="s-a5808f3509"></a>`additionalProperties`: `false`
- <a id="s-0f345b6523"></a>`required`: `["job_id","production_sha256","output_collection","output_bindings"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-236dcd4b3f"></a>`format` | no | type="string"; const="stove0-target-settlement/v1"; default="stove0-target-settlement/v1" |  |
| <a id="s-50a9863786"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-09edab56a9"></a>`output_bindings` | yes | [TargetOutputBindingSetIdentity](#s-897edc1cc2) |  |
| <a id="s-566f0729ca"></a>`output_collection` | yes | [OutputCollectionRef](#s-3255c55702) |  |
| <a id="s-5ad8646018"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [CollectionId](#s-2089952b87)
- [OutputCollectionRef](#s-3255c55702)
- [TargetOutputBindingSetIdentity](#s-897edc1cc2)

##### <a id="s-2089952b87"></a>definition `CollectionId`

- <a id="s-07d5a90bfe"></a>`type`: `"integer"`
- <a id="s-f2c064c706"></a>`minimum`: `1`

##### <a id="s-3255c55702"></a>definition `OutputCollectionRef`

- <a id="s-83527a47a0"></a>`type`: `"object"`
- <a id="s-950ccbfdf9"></a>`additionalProperties`: `false`
- <a id="s-e782a33c42"></a>`required`: `["collection_id","archive_root_sha256","content_identity","derivation_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e7ea1a4775"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-101a534e36"></a>`collection_id` | yes | [CollectionId](#s-2089952b87) |  |
| <a id="s-9b7276968e"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2b702da3ab"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-897edc1cc2"></a>definition `TargetOutputBindingSetIdentity`

- <a id="s-4191ee4ce6"></a>`type`: `"object"`
- <a id="s-ba5b750320"></a>`additionalProperties`: `false`
- <a id="s-8a9e176bec"></a>`required`: `["artifact_count","total_bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e5d1dcc11e"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-49207694f8"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a402379666"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

## Governing policies

- <a id="pa-a790c0249c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetSettlementAuthorityPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4378d5cb48a0170095dfaad94c7f906c34e5abbbb607aca8f84fcfa28781baf2 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        },
        "OutputCollectionRef": {
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
            },
            "derivation_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity",
            "derivation_sha256"
          ],
          "type": "object"
        },
        "TargetOutputBindingSetIdentity": {
          "additionalProperties": false,
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "type": "integer"
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
            "sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-target-settlement/v1",
          "default": "stove0-target-settlement/v1",
          "type": "string"
        },
        "job_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "output_bindings": {
          "$ref": "#/$defs/TargetOutputBindingSetIdentity"
        },
        "output_collection": {
          "$ref": "#/$defs/OutputCollectionRef"
        },
        "production_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "job_id",
        "production_sha256",
        "output_collection",
        "output_bindings"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-target-settlement/v1'] = 'stove0-target-settlement/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], production_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_target_protocol.protocol.OutputCollectionRef, output_bindings: stove0_target_protocol.protocol.TargetOutputBindingSetIdentity) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetSettlementAuthorityPayload",
  "unit": "export"
}
```

</details>
