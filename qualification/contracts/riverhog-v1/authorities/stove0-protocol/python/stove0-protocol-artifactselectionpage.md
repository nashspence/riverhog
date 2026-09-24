# stove0_protocol.ArtifactSelectionPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifactselectionpage:e313fe515e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cce11045fd"></a>
- <a id="s-56d70c655b"></a>`distribution`: `stove0-protocol`
- <a id="s-03e18b6dc0"></a>`module`: `stove0_protocol`
- <a id="s-61c8bc0d62"></a>`name`: `ArtifactSelectionPage`
- <a id="s-ac3d36c57f"></a>`unit`: `export`

### Declared structure

- <a id="s-fac086ee2c"></a>`kind`: `"class"`
- <a id="s-224510dba9"></a>`signature`: `"\"(*, authority: stove0_protocol.fork_join.ArtifactSelectionRef, continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, next_continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, complete: bool, artifacts: Annotated[tuple[stove0_protocol.models.WorkArtifactSubject, ...], MaxLen(max_length=256)]) -> None\""`

#### Validated model schema

<a id="s-73808f3317"></a>

- <a id="s-cb61981483"></a>`type`: `"object"`
- <a id="s-bae1591c6e"></a>`additionalProperties`: `false`
- <a id="s-f241ab95b6"></a>`required`: `["authority","complete","artifacts"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4228ab83c9"></a>`artifacts` | yes | type="array"; items=([WorkArtifactSubject](#s-98ccdd2cef)); maxItems=256; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"selection-bound-start_ordinal","reason":"bounded-artifact-selection-page"} |  |
| <a id="s-a807dd959e"></a>`authority` | yes | [ArtifactSelectionRef](#s-4d88ccaa17) |  |
| <a id="s-2460fa0fde"></a>`complete` | yes | type="boolean" |  |
| <a id="s-c6f2138d13"></a>`continuation` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-5169754d6f"></a>`next_continuation` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |

##### Definitions

- [ArtifactSelectionRef](#s-4d88ccaa17)
- [CollectionId](#s-9da40006db)
- [CollectionRootIdentityRef](#s-206c8c53c3)
- [WorkArtifactSubject](#s-98ccdd2cef)

##### <a id="s-4d88ccaa17"></a>definition `ArtifactSelectionRef`

- <a id="s-c300627963"></a>`type`: `"object"`
- <a id="s-fbc722e3ce"></a>`additionalProperties`: `false`
- <a id="s-f95bbf315c"></a>`required`: `["selection_sha256","artifact_count","total_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cac78848a3"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-95cc4da6e7"></a>`selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bdf2eb65e2"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-9da40006db"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-19e9740f3f"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-034f380eba"></a>2 | not=(const="0") |

##### <a id="s-206c8c53c3"></a>definition `CollectionRootIdentityRef`

- <a id="s-8a168b4638"></a>`type`: `"object"`
- <a id="s-db4c69485c"></a>`additionalProperties`: `false`
- <a id="s-81e6cb5c3d"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a0cce723b5"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5609dfa131"></a>`collection_id` | yes | [CollectionId](#s-9da40006db) |  |
| <a id="s-bb7a1a2b02"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-98ccdd2cef"></a>definition `WorkArtifactSubject`

- <a id="s-16e64efdae"></a>`type`: `"object"`
- <a id="s-b596495245"></a>`additionalProperties`: `false`
- <a id="s-70e15b5672"></a>`required`: `["id","role","collection","path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f3b53cd06f"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-dc55277103"></a>`collection` | yes | [CollectionRootIdentityRef](#s-206c8c53c3) |  |
| <a id="s-4dd449c900"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-036a278b72"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; default=null |  |
| <a id="s-dd4d1cfe3a"></a>`path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-45aabbf4aa"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2246c9e242"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [bind_page](stove0-protocol-artifactselectionpage-bind-page.md)

## Governing policies

- <a id="pa-eb1a09b97a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.ArtifactSelectionPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: afdd9f942df49dbd116db21e2d8dd2a1ee4f286b707e2270ae60c5568fe67fab -->

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
        "WorkArtifactSubject": {
          "additionalProperties": false,
          "properties": {
            "bytes": {
              "minimum": 0,
              "type": "integer"
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
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "ArtifactSelectionPage",
  "unit": "export"
}
```

</details>
