# schemas: ObservationResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-observationresult:732d745985 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 7 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ObservationResult`

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

- [schemas: ArtifactSubject](schemas-artifactsubject.md)
- [schemas: JsonSchemaDocument](schemas-jsonschemadocument.md)
- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: ObservationFailure](schemas-observationfailure.md)
- [schemas: ObservationInapplicable](schemas-observationinapplicable.md)
- [schemas: ObserverImplementation](schemas-observerimplementation.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: ObservationResult
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `execution_evidence` | no | object |  |
| `facts` | no | object (2 fields) |  |
| `facts_schema` | no | object (1 fields) |  |
| `facts_sha256` | no | object (2 fields) |  |
| `failure` | no | object (1 fields) |  |
| `format` | no | string |  |
| `inapplicable` | no | object (1 fields) |  |
| `observer` | yes | #/components/schemas/ObserverImplementation |  |
| `observer_contract_id` | yes | string |  |
| `observer_contract_sha256` | yes | string |  |
| `request_id` | yes | string |  |
| `result_sha256` | yes | string |  |
| `state` | yes | string |  |
| `subjects` | yes | array |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9f06c7aaea3e4c68bf2da3d3f5ef467b30b67cb3db431233ed43d443146613be -->

```json
{
  "additionalProperties": false,
  "properties": {
    "execution_evidence": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Execution Evidence",
      "type": "object"
    },
    "facts": {
      "anyOf": [
        {
          "additionalProperties": {
            "$ref": "#/components/schemas/JsonValue"
          },
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "title": "Facts"
    },
    "facts_schema": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/JsonSchemaDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "facts_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Facts Sha256"
    },
    "failure": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ObservationFailure"
        },
        {
          "type": "null"
        }
      ]
    },
    "format": {
      "const": "stove0-observation-result/v1",
      "default": "stove0-observation-result/v1",
      "title": "Format",
      "type": "string"
    },
    "inapplicable": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ObservationInapplicable"
        },
        {
          "type": "null"
        }
      ]
    },
    "observer": {
      "$ref": "#/components/schemas/ObserverImplementation"
    },
    "observer_contract_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Observer Contract Id",
      "type": "string"
    },
    "observer_contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Observer Contract Sha256",
      "type": "string"
    },
    "request_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Request Id",
      "type": "string"
    },
    "result_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Result Sha256",
      "type": "string"
    },
    "state": {
      "enum": [
        "observed",
        "inapplicable",
        "failed",
        "canceled"
      ],
      "title": "State",
      "type": "string"
    },
    "subjects": {
      "items": {
        "$ref": "#/components/schemas/ArtifactSubject"
      },
      "minItems": 1,
      "title": "Subjects",
      "type": "array"
    }
  },
  "required": [
    "request_id",
    "state",
    "observer",
    "observer_contract_id",
    "observer_contract_sha256",
    "subjects",
    "result_sha256"
  ],
  "title": "ObservationResult",
  "type": "object"
}
```
