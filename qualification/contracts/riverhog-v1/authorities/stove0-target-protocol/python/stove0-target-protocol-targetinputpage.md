# stove0_target_protocol.TargetInputPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetinputpage:b5c9613dd6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-412205faec"></a>
- <a id="s-22e6c8749a"></a>`distribution`: `stove0-target-protocol`
- <a id="s-d46c396912"></a>`module`: `stove0_target_protocol`
- <a id="s-640b511f35"></a>`name`: `TargetInputPage`
- <a id="s-d8bd36b213"></a>`unit`: `export`

### Declared structure

- <a id="s-d39bf980e6"></a>`kind`: `"class"`
- <a id="s-237a789a19"></a>`signature`: `"\"(*, authority: stove0_target_protocol.protocol.TargetInputAuthority, continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, next_continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, complete: bool, artifacts: Annotated[tuple[stove0_target_protocol.protocol.InputArtifact, ...], MaxLen(max_length=256)]) -> None\""`

#### Validated model schema

<a id="s-659cff9b19"></a>

- <a id="s-e81a4de6fc"></a>`type`: `"object"`
- <a id="s-6f1b5ef4f9"></a>`additionalProperties`: `false`
- <a id="s-a607989546"></a>`required`: `["authority","complete","artifacts"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7e84c58d54"></a>`artifacts` | yes | type="array"; items=([InputArtifact](#s-b712976709)); maxItems=256; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"authority-bound-start_ordinal","reason":"bounded-target-input-page"} |  |
| <a id="s-5b256c5ba8"></a>`authority` | yes | [TargetInputAuthority](#s-19d884a805) |  |
| <a id="s-edb25598f6"></a>`complete` | yes | type="boolean" |  |
| <a id="s-538e7ce942"></a>`continuation` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-a0dbd706b2"></a>`next_continuation` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |

##### Definitions

- [ArtifactSelectionRef](#s-2a796fd68b)
- [CollectionId](#s-aacc57599e)
- [CollectionRootRef](#s-15713f5133)
- [InputArtifact](#s-b712976709)
- [TargetInputAuthority](#s-19d884a805)
- [TargetInputRoleCount](#s-35383094e0)

##### <a id="s-2a796fd68b"></a>definition `ArtifactSelectionRef`

- <a id="s-a3d8603ce6"></a>`type`: `"object"`
- <a id="s-58a8a124ba"></a>`additionalProperties`: `false`
- <a id="s-5848a51844"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-52c65ce55d"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-16ebf6fe5e"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-67563dd80e"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-aacc57599e"></a>definition `CollectionId`

- <a id="s-279d878231"></a>`type`: `"integer"`
- <a id="s-722d72ea86"></a>`minimum`: `1`

##### <a id="s-15713f5133"></a>definition `CollectionRootRef`

- <a id="s-e3c1d1bf98"></a>`type`: `"object"`
- <a id="s-ebb2c56db2"></a>`additionalProperties`: `false`
- <a id="s-2ae981970c"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8e54cd07fe"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-15758b9bf1"></a>`collection_id` | yes | [CollectionId](#s-aacc57599e) |  |
| <a id="s-0429a3b526"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-b712976709"></a>definition `InputArtifact`

- <a id="s-7a28e95112"></a>`type`: `"object"`
- <a id="s-a305fa05d4"></a>`additionalProperties`: `false`
- <a id="s-c76fef3096"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0149b5682c"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-ffdfc4b684"></a>`collection` | yes | [CollectionRootRef](#s-15713f5133) |  |
| <a id="s-09470f6876"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-2578e1740a"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-4c8843f4ff"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-fe12d9f0f2"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-18cc219485"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-19d884a805"></a>definition `TargetInputAuthority`

- <a id="s-b6bcd41655"></a>`type`: `"object"`
- <a id="s-b2f521d827"></a>`additionalProperties`: `false`
- <a id="s-288c904ebb"></a>`required`: `["selection","roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c4c7f09ab2"></a>`roles` | yes | type="array"; items=([TargetInputRoleCount](#s-35383094e0)); minItems=1 |  |
| <a id="s-2ed863343d"></a>`selection` | yes | [ArtifactSelectionRef](#s-2a796fd68b) |  |

##### <a id="s-35383094e0"></a>definition `TargetInputRoleCount`

- <a id="s-9d4ebce383"></a>`type`: `"object"`
- <a id="s-f10ee21131"></a>`additionalProperties`: `false`
- <a id="s-77676422e4"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ff167bad6e"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-5029bd8c77"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

## Maintained corroboration

### Related interface records

- [bind_page](stove0-target-protocol-targetinputpage-bind-page.md)

## Governing policies

- <a id="pa-bb4fb25791"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetInputPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6bbafc318c502cc739a6da17f1739021ddea63e6f348d731d3197183410c52c2 -->

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
        },
        "InputArtifact": {
          "additionalProperties": false,
          "properties": {
            "bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "collection": {
              "$ref": "#/$defs/CollectionRootRef"
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
        },
        "TargetInputAuthority": {
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
        "artifacts": {
          "items": {
            "$ref": "#/$defs/InputArtifact"
          },
          "maxItems": 256,
          "type": "array",
          "x-riverhog-extent": {
            "policy": "segmented_no_total_max",
            "progression": "authority-bound-start_ordinal",
            "reason": "bounded-target-input-page"
          }
        },
        "authority": {
          "$ref": "#/$defs/TargetInputAuthority"
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
    "signature": "\"(*, authority: stove0_target_protocol.protocol.TargetInputAuthority, continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, next_continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, complete: bool, artifacts: Annotated[tuple[stove0_target_protocol.protocol.InputArtifact, ...], MaxLen(max_length=256)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetInputPage",
  "unit": "export"
}
```

</details>
