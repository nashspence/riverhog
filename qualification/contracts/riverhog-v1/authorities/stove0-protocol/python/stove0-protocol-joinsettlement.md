# stove0_protocol.JoinSettlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinsettlement:abf3616a3d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dcd3714db2"></a>
- <a id="s-ed5fae3176"></a>`distribution`: `stove0-protocol`
- <a id="s-5f75ac3d4e"></a>`module`: `stove0_protocol`
- <a id="s-d76249ec7a"></a>`name`: `JoinSettlement`
- <a id="s-0cf8849cc9"></a>`unit`: `export`

### Declared structure

- <a id="s-a9fe769a35"></a>`kind`: `"class"`
- <a id="s-202cfb6d6d"></a>`signature`: `"\"(*, format: Literal['stove0-join-settlement/v1'] = 'stove0-join-settlement/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], producer_settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_protocol.models.CollectionRootRef, output_selection: stove0_protocol.fork_join.ArtifactSelectionRef, settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-0b32b7a2dc"></a>

- <a id="s-c93221e533"></a>`type`: `"object"`
- <a id="s-bba4d45bcf"></a>`additionalProperties`: `false`
- <a id="s-715c1ea148"></a>`required`: `["work_id","workflow_plan_sha256","join_plan_sha256","derivation_sha256","producer_settlement_sha256","output_collection","output_selection","settlement_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c5967c3618"></a>`derivation_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-2bf52a28a7"></a>`format` | no | type="string"; const="stove0-join-settlement/v1"; default="stove0-join-settlement/v1" |  |
| <a id="s-e8c99719c7"></a>`join_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b6641a0702"></a>`output_collection` | yes | [CollectionRootRef](#s-314b13a04f) |  |
| <a id="s-0dffa823d1"></a>`output_selection` | yes | [ArtifactSelectionRef](#s-09d3fb4dae) |  |
| <a id="s-468a4eb04b"></a>`producer_settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b849bc92a7"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c77cad58ef"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-030f98d42e"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [ArtifactSelectionRef](#s-09d3fb4dae)
- [CollectionId](#s-f30386e37f)
- [CollectionRootRef](#s-314b13a04f)

##### <a id="s-09d3fb4dae"></a>definition `ArtifactSelectionRef`

- <a id="s-d4c58fa294"></a>`type`: `"object"`
- <a id="s-4557fc7982"></a>`additionalProperties`: `false`
- <a id="s-00a469b8ff"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6f28036908"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-872d49f200"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fa0db39064"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-f30386e37f"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-c2488cc8e6"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-0108558e80"></a>2 | not=(const="0") |

##### <a id="s-314b13a04f"></a>definition `CollectionRootRef`

- <a id="s-5e42cd9dcf"></a>`type`: `"object"`
- <a id="s-e10e0c342c"></a>`additionalProperties`: `false`
- <a id="s-6a651848b3"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a42211b8fa"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c4c5706b13"></a>`collection_id` | yes | [CollectionId](#s-f30386e37f) |  |
| <a id="s-338642f94f"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [seal](stove0-protocol-joinsettlement-seal.md)
- [verify_digest](stove0-protocol-joinsettlement-verify-digest.md)

## Governing policies

- <a id="pa-729e033f64"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JoinSettlement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ef5d2f07734e24300f492692fc78ae320d0bed1f56215afb5e068c432c70daac -->

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
        "format": {
          "const": "stove0-join-settlement/v1",
          "default": "stove0-join-settlement/v1",
          "type": "string"
        },
        "join_plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
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
        "work_id",
        "workflow_plan_sha256",
        "join_plan_sha256",
        "derivation_sha256",
        "producer_settlement_sha256",
        "output_collection",
        "output_selection",
        "settlement_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-join-settlement/v1'] = 'stove0-join-settlement/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], producer_settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_protocol.models.CollectionRootRef, output_selection: stove0_protocol.fork_join.ArtifactSelectionRef, settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JoinSettlement",
  "unit": "export"
}
```

</details>
