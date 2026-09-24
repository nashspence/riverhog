# stove0_operator_contracts.ArtifactSelectionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-artifactselectionpage:cfac71f202 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b4744a6bbb"></a>
- <a id="s-6f18d8a8ef"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-681fc212d5"></a>`module`: `stove0_operator_contracts`
- <a id="s-0467b1777a"></a>`name`: `ArtifactSelectionPage`
- <a id="s-78451bb7b2"></a>`unit`: `export`

### Declared structure

- <a id="s-38150fe659"></a>`kind`: `"class"`
- <a id="s-b7ce2efa07"></a>`signature`: `"\"(*, authority: stove0_protocol.fork_join.ArtifactSelectionRef, continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, next_continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, complete: bool, artifacts: Annotated[tuple[stove0_protocol.models.WorkArtifactSubject, ...], MaxLen(max_length=256)]) -> None\""`

#### Validated model schema

<a id="s-49ab52d463"></a>

- <a id="s-71784ef16b"></a>`type`: `"object"`
- <a id="s-c0eaa86f70"></a>`additionalProperties`: `false`
- <a id="s-ae2cebb4ed"></a>`required`: `["authority","complete","artifacts"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dbd8166b32"></a>`artifacts` | yes | type="array"; items=([WorkArtifactSubject](#s-71efee5497)); maxItems=256; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"selection-bound-start_ordinal","reason":"bounded-artifact-selection-page"} |  |
| <a id="s-c8519fd604"></a>`authority` | yes | [ArtifactSelectionRef](#s-612021a3f8) |  |
| <a id="s-f3985b237a"></a>`complete` | yes | type="boolean" |  |
| <a id="s-61fc05368d"></a>`continuation` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-30a978c4b5"></a>`next_continuation` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |

##### Definitions

- [ArtifactSelectionRef](#s-612021a3f8)
- [CollectionId](#s-637edf639e)
- [CollectionRootIdentityRef](#s-9dd8916c22)
- [NonnegativeDecimal](#s-508119444c)
- [WorkArtifactSubject](#s-71efee5497)

##### <a id="s-612021a3f8"></a>definition `ArtifactSelectionRef`

- <a id="s-9708c70dcc"></a>`type`: `"object"`
- <a id="s-d8970c2f5e"></a>`additionalProperties`: `false`
- <a id="s-95cd2d282e"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d394fb634c"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-00663ee589"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-66d3103a03"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-508119444c); ge=0 |  |

##### <a id="s-637edf639e"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-b2dd4ad27d"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-0bd3b1ec86"></a>2 | not=(const="0") |

##### <a id="s-9dd8916c22"></a>definition `CollectionRootIdentityRef`

- <a id="s-587034c0a2"></a>`type`: `"object"`
- <a id="s-9ec494e70a"></a>`additionalProperties`: `false`
- <a id="s-bf7dca19bb"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2091223582"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0c46eada96"></a>`collection_id` | yes | [CollectionId](#s-637edf639e) |  |
| <a id="s-7bdcea6694"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-508119444c"></a>definition `NonnegativeDecimal`

- <a id="s-528deabe34"></a>`type`: `"string"`
- <a id="s-e041b8d024"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-71efee5497"></a>definition `WorkArtifactSubject`

- <a id="s-4a768fcbce"></a>`type`: `"object"`
- <a id="s-89e49c304e"></a>`additionalProperties`: `false`
- <a id="s-b46ca162c0"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0e4f53d9ce"></a>`bytes` | yes | [NonnegativeDecimal](#s-508119444c); ge=0 |  |
| <a id="s-fc0dd35720"></a>`collection` | yes | [CollectionRootIdentityRef](#s-9dd8916c22) |  |
| <a id="s-b0f5ebd2ad"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-eb23215221"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-5a321e60b5"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-e4756a16f3"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-dc37ddee79"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [bind_page](stove0-operator-contracts-artifactselectionpage-bind-page.md)

## Governing policies

- <a id="pa-8210bec093"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.ArtifactSelectionPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b55058e450aaea99a3d78ec5715543e4c7132d32a764b8643c0bd80f109aaf0f -->

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
        },
        "WorkArtifactSubject": {
          "additionalProperties": false,
          "properties": {
            "bytes": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 0
            },
            "collection": {
              "$ref": "#/$defs/CollectionRootIdentityRef"
            },
            "id": {
              "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
              "type": "string"
            },
            "media_type": {
              "anyOf": [
                {
                  "maxLength": 255,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "path": {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "role",
            "collection",
            "path",
            "bytes",
            "sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "artifacts": {
          "items": {
            "$ref": "#/$defs/WorkArtifactSubject"
          },
          "maxItems": 256,
          "type": "array",
          "x-riverhog-extent": {
            "policy": "segmented_no_total_max",
            "progression": "selection-bound-start_ordinal",
            "reason": "bounded-artifact-selection-page"
          }
        },
        "authority": {
          "$ref": "#/$defs/ArtifactSelectionRef"
        },
        "complete": {
          "type": "boolean"
        },
        "continuation": {
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
        "next_continuation": {
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
        }
      },
      "required": [
        "authority",
        "complete",
        "artifacts"
      ],
      "type": "object"
    },
    "signature": "\"(*, authority: stove0_protocol.fork_join.ArtifactSelectionRef, continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, next_continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, complete: bool, artifacts: Annotated[tuple[stove0_protocol.models.WorkArtifactSubject, ...], MaxLen(max_length=256)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "ArtifactSelectionPage",
  "unit": "export"
}
```

</details>
