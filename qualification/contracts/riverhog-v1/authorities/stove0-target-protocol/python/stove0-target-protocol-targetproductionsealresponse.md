# stove0_target_protocol.TargetProductionSealResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetproductionsealresponse:9869c93ab2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1f58b64da0"></a>
- <a id="s-2f60e9ad2f"></a>`distribution`: `stove0-target-protocol`
- <a id="s-f7bc6d24dc"></a>`module`: `stove0_target_protocol`
- <a id="s-144d46ffa2"></a>`name`: `TargetProductionSealResponse`
- <a id="s-44ae8fa07d"></a>`unit`: `export`

### Declared structure

- <a id="s-da85121f2b"></a>`kind`: `"class"`
- <a id="s-5e1b7290fc"></a>`signature`: `"\"(*, state: Literal['sealing', 'sealed'], production: stove0_target_protocol.protocol.TargetProductionAuthority \| None = None) -> None\""`

#### Validated model schema

<a id="s-4c50fedfd8"></a>

- <a id="s-f4259f2c23"></a>`type`: `"object"`
- <a id="s-73b4118d16"></a>`additionalProperties`: `false`
- <a id="s-54acb3d010"></a>`required`: `["state"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c7ad624559"></a>`production` | no | anyOf=[([TargetProductionAuthority](#s-bff5bb50a6)); (type="null")]; default=null |  |
| <a id="s-9ce295c9e8"></a>`state` | yes | type="string"; enum=["sealing","sealed"] |  |

##### Definitions

- [ArtifactDispositionSetIdentity](#s-9c9aa1be20)
- [NonnegativeDecimal](#s-0386c185e7)
- [OutputArtifactRoleCount](#s-bbf2019ff9)
- [OutputArtifactSetIdentity](#s-1c7cabfa19)
- [TargetProductionAuthority](#s-bff5bb50a6)

##### <a id="s-9c9aa1be20"></a>definition `ArtifactDispositionSetIdentity`

- <a id="s-1b2bdfd248"></a>`type`: `"object"`
- <a id="s-708b09f66c"></a>`required`: `["disposition_count","output_edge_count","output_artifact_count","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-66e9c4436a"></a>`disposition_count` | yes | type="integer" |  |
| <a id="s-a5b418bffa"></a>`output_artifact_count` | yes | type="integer" |  |
| <a id="s-8add1768f3"></a>`output_edge_count` | yes | type="integer" |  |
| <a id="s-9c421aec49"></a>`sha256` | yes | type="string" |  |

##### <a id="s-0386c185e7"></a>definition `NonnegativeDecimal`

- <a id="s-0585395a12"></a>`type`: `"string"`
- <a id="s-288c975d8b"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-bbf2019ff9"></a>definition `OutputArtifactRoleCount`

- <a id="s-99bd1d4cd9"></a>`type`: `"object"`
- <a id="s-5664900f34"></a>`additionalProperties`: `false`
- <a id="s-ae024a3a7c"></a>`required`: `["role","count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2b9161706f"></a>`count` | yes | type="integer"; minimum=1 |  |
| <a id="s-e2e2f8d9c8"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-1c7cabfa19"></a>definition `OutputArtifactSetIdentity`

- <a id="s-7439f966b5"></a>`type`: `"object"`
- <a id="s-645292c94f"></a>`additionalProperties`: `false`
- <a id="s-e187ce701d"></a>`required`: `["artifact_count","total_bytes","roles","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1fde104105"></a>`artifact_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-d8ab5dbc9a"></a>`roles` | yes | type="array"; items=([OutputArtifactRoleCount](#s-bbf2019ff9)); minItems=1 |  |
| <a id="s-0b03c441e2"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b91359d34e"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-0386c185e7); ge=0 |  |

##### <a id="s-bff5bb50a6"></a>definition `TargetProductionAuthority`

- <a id="s-cef6971f1e"></a>`type`: `"object"`
- <a id="s-8563090356"></a>`additionalProperties`: `false`
- <a id="s-537343f925"></a>`required`: `["job_id","plan_sha256","outputs","disposition_count","disposition_sha256","source_edge_count","source_edge_sha256","riverhog_disposition_set","production_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a28b6fe505"></a>`disposition_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-21ae1f17a9"></a>`disposition_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-024b4d6bb9"></a>`format` | no | type="string"; const="stove0-target-production/v1"; default="stove0-target-production/v1" |  |
| <a id="s-7aa163fcc7"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-059d28556e"></a>`outputs` | yes | [OutputArtifactSetIdentity](#s-1c7cabfa19) |  |
| <a id="s-cce0890032"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-bd2cd6c0e0"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-3d66bfa93c"></a>`riverhog_disposition_set` | yes | [ArtifactDispositionSetIdentity](#s-9c9aa1be20) |  |
| <a id="s-826bbd8d3d"></a>`source_edge_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-16c8e4edd6"></a>`source_edge_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [validate_state](stove0-target-protocol-targetproductionsealresponse-validate-state.md)

## Governing policies

- <a id="pa-fec36f73c1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetProductionSealResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d1fc65066f13086b37f96b4722d43ff7a7fa96ee1c370a693f89e42c77a73d93 -->

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
        },
        "TargetProductionAuthority": {
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
        }
      },
      "additionalProperties": false,
      "properties": {
        "production": {
          "anyOf": [
            {
              "$ref": "#/$defs/TargetProductionAuthority"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "state": {
          "enum": [
            "sealing",
            "sealed"
          ],
          "type": "string"
        }
      },
      "required": [
        "state"
      ],
      "type": "object"
    },
    "signature": "\"(*, state: Literal['sealing', 'sealed'], production: stove0_target_protocol.protocol.TargetProductionAuthority | None = None) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetProductionSealResponse",
  "unit": "export"
}
```

</details>
