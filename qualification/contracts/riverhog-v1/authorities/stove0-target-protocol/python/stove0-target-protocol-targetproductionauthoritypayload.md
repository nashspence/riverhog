# stove0_target_protocol.TargetProductionAuthorityPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetproductionau-bd5eaa29a7:a3fb481714 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7fe32289dc"></a>
- <a id="s-24148d2bed"></a>`distribution`: `stove0-target-protocol`
- <a id="s-2ab5d8d881"></a>`module`: `stove0_target_protocol`
- <a id="s-89cd56c52c"></a>`name`: `TargetProductionAuthorityPayload`
- <a id="s-f860e544d6"></a>`unit`: `export`

### Declared structure

- <a id="s-f7cdc40587"></a>`kind`: `"class"`
- <a id="s-72eedd2238"></a>`signature`: `"\"(*, format: Literal['stove0-target-production/v1'] = 'stove0-target-production/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], outputs: stove0_target_protocol.protocol.OutputArtifactSetIdentity, disposition_count: Annotated[int, Ge(ge=1)], disposition_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], source_edge_count: Annotated[int, Ge(ge=1)], source_edge_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], riverhog_disposition_set: riverhog_protocol.collection_workflows.ArtifactDispositionSetIdentity) -> None\""`

#### Validated model schema

<a id="s-57f28a42cf"></a>

- <a id="s-04050e441b"></a>`type`: `"object"`
- <a id="s-4626907ab3"></a>`additionalProperties`: `false`
- <a id="s-835967bdba"></a>`required`: `["job_id","plan_sha256","outputs","disposition_count","disposition_sha256","source_edge_count","source_edge_sha256","riverhog_disposition_set"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5879b61adf"></a>`disposition_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-cfff7d10dc"></a>`disposition_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8fdba92d1b"></a>`format` | no | type="string"; const="stove0-target-production/v1"; default="stove0-target-production/v1" |  |
| <a id="s-22fdc5cb2f"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9a75b7130a"></a>`outputs` | yes | [OutputArtifactSetIdentity](#s-215cbcc0e0) |  |
| <a id="s-5c74ff61de"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9a0c84cd62"></a>`riverhog_disposition_set` | yes | [ArtifactDispositionSetIdentity](#s-c3ddafe1b4) |  |
| <a id="s-661e837fdf"></a>`source_edge_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-c62aae2f9b"></a>`source_edge_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [ArtifactDispositionSetIdentity](#s-c3ddafe1b4)
- [OutputArtifactRoleCount](#s-15e65d7f01)
- [OutputArtifactSetIdentity](#s-215cbcc0e0)

##### <a id="s-c3ddafe1b4"></a>definition `ArtifactDispositionSetIdentity`

- <a id="s-803fb24f65"></a>`type`: `"object"`
- <a id="s-a14102dd20"></a>`required`: `["disposition_count","output_edge_count","output_artifact_count","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-61534a7416"></a>`disposition_count` | yes | type="integer" |  |
| <a id="s-6f9fae2b2b"></a>`output_artifact_count` | yes | type="integer" |  |
| <a id="s-46e47cac8b"></a>`output_edge_count` | yes | type="integer" |  |
| <a id="s-2889cb2d85"></a>`sha256` | yes | type="string" |  |

##### <a id="s-15e65d7f01"></a>definition `OutputArtifactRoleCount`

- <a id="s-6b05d16fcd"></a>`type`: `"object"`
- <a id="s-a234cfdb82"></a>`additionalProperties`: `false`
- <a id="s-0efd7dca1b"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9049877fa6"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-6262e7ad19"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-215cbcc0e0"></a>definition `OutputArtifactSetIdentity`

- <a id="s-b1bf786ec2"></a>`type`: `"object"`
- <a id="s-3128c2084f"></a>`additionalProperties`: `false`
- <a id="s-0f04e233a2"></a>`required`: `["artifact_count","total_bytes","roles","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bfdc292426"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-066b6ed4df"></a>`roles` | yes | type="array"; items=([OutputArtifactRoleCount](#s-15e65d7f01)); minItems=1 |  |
| <a id="s-08421f580c"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-650d7cfda7"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

## Governing policies

- <a id="pa-eaa6d9a059"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetProductionAuthorityPayload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ea63661c181d20a125141ce9299a64cda6fc324f6f3f073bd8f172e9ffbac58e -->

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
              "minimum": 0,
              "type": "integer"
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
        "riverhog_disposition_set"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-target-production/v1'] = 'stove0-target-production/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], outputs: stove0_target_protocol.protocol.OutputArtifactSetIdentity, disposition_count: Annotated[int, Ge(ge=1)], disposition_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], source_edge_count: Annotated[int, Ge(ge=1)], source_edge_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], riverhog_disposition_set: riverhog_protocol.collection_workflows.ArtifactDispositionSetIdentity) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetProductionAuthorityPayload",
  "unit": "export"
}
```

</details>
