# schemas: ObserverUse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-observeruse:772a5a6a56 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ObserverUse`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactRule](schemas-artifactrule.md)
- [schemas: JsonValue](schemas-jsonvalue.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=67108864, minimum=1, reason=schema-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `contract_max` | maximum=86400, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: ObserverUse
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifact_rules` | no | array |  |
| `contract_id` | yes | string |  |
| `contract_sha256` | yes | string |  |
| `maximum_result_bytes` | no | integer |  |
| `options` | no | object |  |
| `registration_id` | yes | string |  |
| `retrieval_policy` | no | string |  |
| `timeout_seconds` | no | integer |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 980cd90a66ccdcdbd0d8a24a5f1ae06e4ecf6f268c9e89d2cfb1a5cc662c3b87 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "artifact_rules": {
      "default": [
        {
          "glob": "*",
          "role": "stove0.source/v1"
        }
      ],
      "items": {
        "$ref": "#/components/schemas/ArtifactRule"
      },
      "title": "Artifact Rules",
      "type": "array"
    },
    "contract_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Contract Id",
      "type": "string"
    },
    "contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Contract Sha256",
      "type": "string"
    },
    "maximum_result_bytes": {
      "default": 1048576,
      "maximum": 67108864,
      "minimum": 1,
      "title": "Maximum Result Bytes",
      "type": "integer"
    },
    "options": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Options",
      "type": "object"
    },
    "registration_id": {
      "title": "Registration Id",
      "type": "string"
    },
    "retrieval_policy": {
      "default": "available-only",
      "enum": [
        "available-only",
        "allow"
      ],
      "title": "Retrieval Policy",
      "type": "string"
    },
    "timeout_seconds": {
      "default": 300,
      "maximum": 86400,
      "minimum": 1,
      "title": "Timeout Seconds",
      "type": "integer"
    }
  },
  "required": [
    "registration_id",
    "contract_id",
    "contract_sha256"
  ],
  "title": "ObserverUse",
  "type": "object"
}
```
