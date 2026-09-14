# stove0_target_protocol.TargetProductionAuthorityPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetproductionau-bd5eaa29a7:a3fb481714 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-baeb2fbfdb"></a>`title`: TargetProductionAuthorityPayload
- <a id="s-04050e441b"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5879b61adf"></a>`disposition_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-cfff7d10dc"></a>`disposition_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-8fdba92d1b"></a>`format` | no | type="string"; const="stove0-target-production/v1" |  |
| <a id="s-22fdc5cb2f"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9a75b7130a"></a>`outputs` | yes | #/$defs/OutputArtifactSetIdentity |  |
| <a id="s-5c74ff61de"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9a0c84cd62"></a>`riverhog_disposition_set` | yes | #/$defs/ArtifactDispositionSetIdentity |  |
| <a id="s-661e837fdf"></a>`source_edge_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-c62aae2f9b"></a>`source_edge_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-c3ddafe1b4"></a>`ArtifactDispositionSetIdentity` | type="object"; fields=`disposition_count`, `output_artifact_count`, `output_edge_count`, `sha256`; additional keys=`required` |
| <a id="s-15e65d7f01"></a>`OutputArtifactRoleCount` | type="object"; fields=`count`, `role`; additional keys=`additionalProperties`, `required` |
| <a id="s-215cbcc0e0"></a>`OutputArtifactSetIdentity` | type="object"; fields=`artifact_count`, `roles`, `sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-eaa6d9a059"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetProductionAuthorityPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6540b069aeae0a377550c3a673382b0d82038577acc550563318051e12debcfd -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactDispositionSetIdentity": {
          "description": "Small identity for one sealed claim-scoped relational disposition set.",
          "properties": {
            "disposition_count": {
              "title": "Disposition Count",
              "type": "integer"
            },
            "output_artifact_count": {
              "title": "Output Artifact Count",
              "type": "integer"
            },
            "output_edge_count": {
              "title": "Output Edge Count",
              "type": "integer"
            },
            "sha256": {
              "title": "Sha256",
              "type": "string"
            }
          },
          "required": [
            "disposition_count",
            "output_edge_count",
            "output_artifact_count",
            "sha256"
          ],
          "title": "ArtifactDispositionSetIdentity",
          "type": "object"
        },
        "OutputArtifactRoleCount": {
          "additionalProperties": false,
          "properties": {
            "count": {
              "minimum": 1,
              "title": "Count",
              "type": "integer"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "title": "Role",
              "type": "string"
            }
          },
          "required": [
            "role",
            "count"
          ],
          "title": "OutputArtifactRoleCount",
          "type": "object"
        },
        "OutputArtifactSetIdentity": {
          "additionalProperties": false,
          "description": "Small identity for target outputs already registered with Riverhog.",
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "title": "Artifact Count",
              "type": "integer"
            },
            "roles": {
              "items": {
                "$ref": "#/$defs/OutputArtifactRoleCount"
              },
              "minItems": 1,
              "title": "Roles",
              "type": "array"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Sha256",
              "type": "string"
            },
            "total_bytes": {
              "minimum": 0,
              "title": "Total Bytes",
              "type": "integer"
            }
          },
          "required": [
            "artifact_count",
            "total_bytes",
            "roles",
            "sha256"
          ],
          "title": "OutputArtifactSetIdentity",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "disposition_count": {
          "minimum": 1,
          "title": "Disposition Count",
          "type": "integer"
        },
        "disposition_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Disposition Sha256",
          "type": "string"
        },
        "format": {
          "const": "stove0-target-production/v1",
          "default": "stove0-target-production/v1",
          "title": "Format",
          "type": "string"
        },
        "job_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Job Id",
          "type": "string"
        },
        "outputs": {
          "$ref": "#/$defs/OutputArtifactSetIdentity"
        },
        "plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Plan Sha256",
          "type": "string"
        },
        "riverhog_disposition_set": {
          "$ref": "#/$defs/ArtifactDispositionSetIdentity"
        },
        "source_edge_count": {
          "minimum": 1,
          "title": "Source Edge Count",
          "type": "integer"
        },
        "source_edge_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Source Edge Sha256",
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
      "title": "TargetProductionAuthorityPayload",
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
