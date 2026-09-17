# stove0_target_protocol.TargetSettlementAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetsettlementauthority:ff60d0a891 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c1ee207cf0"></a>
- <a id="s-fba0b778d8"></a>`distribution`: `stove0-target-protocol`
- <a id="s-6f0ed47037"></a>`module`: `stove0_target_protocol`
- <a id="s-2f38e4e74e"></a>`name`: `TargetSettlementAuthority`
- <a id="s-b4bacbc001"></a>`unit`: `export`

### Declared structure

- <a id="s-3f5932068e"></a>`kind`: `"class"`
- <a id="s-dbe5061253"></a>`signature`: `"\"(*, format: Literal['stove0-target-settlement/v1'] = 'stove0-target-settlement/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], production_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_target_protocol.protocol.OutputCollectionRef, output_bindings: stove0_target_protocol.protocol.TargetOutputBindingSetIdentity, settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-d59596236e"></a>

- <a id="s-3416372493"></a>`type`: `"object"`
- <a id="s-f6e81f774e"></a>`additionalProperties`: `false`
- <a id="s-82731c193d"></a>`required`: `["job_id","production_sha256","output_collection","output_bindings","settlement_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3b5e1f8f22"></a>`format` | no | type="string"; const="stove0-target-settlement/v1"; default="stove0-target-settlement/v1" |  |
| <a id="s-3a5cb0c379"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8112ee66f8"></a>`output_bindings` | yes | [TargetOutputBindingSetIdentity](#s-dba58e77b2) |  |
| <a id="s-ae05f04ff3"></a>`output_collection` | yes | [OutputCollectionRef](#s-41ab79e924) |  |
| <a id="s-769b87df8c"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5f8d947e62"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [CollectionId](#s-9bdbe25a31)
- [OutputCollectionRef](#s-41ab79e924)
- [TargetOutputBindingSetIdentity](#s-dba58e77b2)

##### <a id="s-9bdbe25a31"></a>definition `CollectionId`

- <a id="s-8eb94c1b58"></a>`type`: `"integer"`
- <a id="s-349158864a"></a>`minimum`: `1`

##### <a id="s-41ab79e924"></a>definition `OutputCollectionRef`

- <a id="s-ae081b5a51"></a>`type`: `"object"`
- <a id="s-d5aff07c63"></a>`additionalProperties`: `false`
- <a id="s-a5d2e9e773"></a>`required`: `["collection_id","archive_root_sha256","content_identity","derivation_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0b8b5955eb"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c4ebbe4701"></a>`collection_id` | yes | [CollectionId](#s-9bdbe25a31) |  |
| <a id="s-7cf17ae28d"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-14dfbc72f2"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-dba58e77b2"></a>definition `TargetOutputBindingSetIdentity`

- <a id="s-c0babe7b0e"></a>`type`: `"object"`
- <a id="s-fd001afba7"></a>`additionalProperties`: `false`
- <a id="s-008288fc86"></a>`required`: `["artifact_count","total_bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3d61164193"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-d119557e8d"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bb19dc7dfa"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Related interface records

- [verify_digest](stove0-target-protocol-targetsettlementauthority-verify-digest.md)
- [seal](stove0-target-protocol-targetsettlementauthority-seal.md)

## Governing policies

- <a id="pa-cff6f2da7f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetSettlementAuthority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eb60940f7120f3927082f9fb4c2af4a3acdea9035157eb04153f44cce8200b23 -->

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
        },
        "settlement_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "job_id",
        "production_sha256",
        "output_collection",
        "output_bindings",
        "settlement_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-target-settlement/v1'] = 'stove0-target-settlement/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], production_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_target_protocol.protocol.OutputCollectionRef, output_bindings: stove0_target_protocol.protocol.TargetOutputBindingSetIdentity, settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetSettlementAuthority",
  "unit": "export"
}
```

</details>
