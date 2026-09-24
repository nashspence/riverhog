# stove0_target_protocol.TargetProductionAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetproductionauthority:eb77550338 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3500257307"></a>
- <a id="s-f4cae559bd"></a>`distribution`: `stove0-target-protocol`
- <a id="s-bbc83daa53"></a>`module`: `stove0_target_protocol`
- <a id="s-13bec5d0df"></a>`name`: `TargetProductionAuthority`
- <a id="s-2a882066c4"></a>`unit`: `export`

### Declared structure

- <a id="s-bdd628abe8"></a>`kind`: `"class"`
- <a id="s-cff4a68762"></a>`signature`: `"\"(*, format: Literal['stove0-target-production/v1'] = 'stove0-target-production/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], outputs: stove0_target_protocol.protocol.OutputArtifactSetIdentity, disposition_count: Annotated[int, Ge(ge=1)], disposition_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], source_edge_count: Annotated[int, Ge(ge=1)], source_edge_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], riverhog_disposition_set: riverhog_protocol.collection_workflows.ArtifactDispositionSetIdentity, production_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-f5ebeab6a0"></a>

- <a id="s-1ffcb617c3"></a>`type`: `"object"`
- <a id="s-f0596f3688"></a>`additionalProperties`: `false`
- <a id="s-3eb0f65bd6"></a>`required`: `["job_id","plan_sha256","outputs","disposition_count","disposition_sha256","source_edge_count","source_edge_sha256","riverhog_disposition_set","production_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f92593ed06"></a>`disposition_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-68ce05550c"></a>`disposition_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1e4d7d8e91"></a>`format` | no | type="string"; const="stove0-target-production/v1"; default="stove0-target-production/v1" |  |
| <a id="s-396e979e8a"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0498c0b75a"></a>`outputs` | yes | [OutputArtifactSetIdentity](#s-e500020a28) |  |
| <a id="s-b044cb5bfe"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1537270c7d"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-79a13546f8"></a>`riverhog_disposition_set` | yes | [ArtifactDispositionSetIdentity](#s-d6279287da) |  |
| <a id="s-6572928216"></a>`source_edge_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-a963d7e7ac"></a>`source_edge_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [ArtifactDispositionSetIdentity](#s-d6279287da)
- [NonnegativeDecimal](#s-914e520a33)
- [OutputArtifactRoleCount](#s-7fea261e65)
- [OutputArtifactSetIdentity](#s-e500020a28)

##### <a id="s-d6279287da"></a>definition `ArtifactDispositionSetIdentity`

- <a id="s-040d039ee7"></a>`type`: `"object"`
- <a id="s-204272dc5e"></a>`required`: `["disposition_count","output_edge_count","output_artifact_count","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1922295718"></a>`disposition_count` | yes | type="integer" |  |
| <a id="s-889aba54be"></a>`output_artifact_count` | yes | type="integer" |  |
| <a id="s-144223f8d8"></a>`output_edge_count` | yes | type="integer" |  |
| <a id="s-25b2b89886"></a>`sha256` | yes | type="string" |  |

##### <a id="s-914e520a33"></a>definition `NonnegativeDecimal`

- <a id="s-c77d8a8d54"></a>`type`: `"string"`
- <a id="s-3977471f9a"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-7fea261e65"></a>definition `OutputArtifactRoleCount`

- <a id="s-c647c1855d"></a>`type`: `"object"`
- <a id="s-5bb8a0e0ec"></a>`additionalProperties`: `false`
- <a id="s-ab88c08807"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4287a22c4b"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-72602c21d7"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-e500020a28"></a>definition `OutputArtifactSetIdentity`

- <a id="s-870f1b4a08"></a>`type`: `"object"`
- <a id="s-9801f29d6b"></a>`additionalProperties`: `false`
- <a id="s-bf6c2dc948"></a>`required`: `["artifact_count","total_bytes","roles","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a257bde9d6"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-296156a472"></a>`roles` | yes | type="array"; items=([OutputArtifactRoleCount](#s-7fea261e65)); minItems=1 |  |
| <a id="s-2c5bb14fab"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-64a5feb387"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-914e520a33); ge=0 |  |

## Maintained corroboration

### Related interface records

- [seal](stove0-target-protocol-targetproductionauthority-seal.md)
- [verify_digest](stove0-target-protocol-targetproductionauthority-verify-digest.md)

## Governing policies

- <a id="pa-dd055d8e9c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetProductionAuthority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 35a67232405cf15bc703485dacaf9e93d5966618ae9cf432d61347ae063e0da0 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactDispositionSetIdentity": {
          "properties": {
            "disposition_count": {
              "type": "integer"
            },
            "output_artifact_count": {
              "type": "integer"
            },
            "output_edge_count": {
              "type": "integer"
            },
            "sha256": {
              "type": "string"
            }
          },
          "required": [
            "disposition_count",
            "output_edge_count",
            "output_artifact_count",
            "sha256"
          ],
          "type": "object"
        },
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        },
        "OutputArtifactRoleCount": {
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
        },
        "OutputArtifactSetIdentity": {
          "additionalProperties": false,
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "type": "integer"
            },
            "roles": {
              "items": {
                "$ref": "#/$defs/OutputArtifactRoleCount"
              },
              "minItems": 1,
              "type": "array"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "total_bytes": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 0
            }
          },
          "required": [
            "artifact_count",
            "total_bytes",
            "roles",
            "sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "disposition_count": {
          "minimum": 1,
          "type": "integer"
        },
        "disposition_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "format": {
          "const": "stove0-target-production/v1",
          "default": "stove0-target-production/v1",
          "type": "string"
        },
        "job_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "outputs": {
          "$ref": "#/$defs/OutputArtifactSetIdentity"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "production_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "riverhog_disposition_set": {
          "$ref": "#/$defs/ArtifactDispositionSetIdentity"
        },
        "source_edge_count": {
          "minimum": 1,
          "type": "integer"
        },
        "source_edge_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "job_id",
        "plan_sha256",
        "outputs",
        "disposition_count",
        "disposition_sha256",
        "source_edge_count",
        "source_edge_sha256",
        "riverhog_disposition_set",
        "production_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-target-production/v1'] = 'stove0-target-production/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], outputs: stove0_target_protocol.protocol.OutputArtifactSetIdentity, disposition_count: Annotated[int, Ge(ge=1)], disposition_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], source_edge_count: Annotated[int, Ge(ge=1)], source_edge_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], riverhog_disposition_set: riverhog_protocol.collection_workflows.ArtifactDispositionSetIdentity, production_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetProductionAuthority",
  "unit": "export"
}
```

</details>
