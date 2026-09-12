# schemas: ObservationResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-observationresult:732d745985 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 7 |

## External contract

- `title`: ObservationResult
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `execution_evidence` | no | type="object"; additional keys=`additionalProperties` |  |
| `facts` | no | anyOf=type="object"; additional keys=`additionalProperties` \| type="null" |  |
| `facts_schema` | no | anyOf=#/components/schemas/JsonSchemaDocument \| type="null" |  |
| `facts_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| `failure` | no | anyOf=#/components/schemas/ObservationFailure \| type="null" |  |
| `format` | no | type="string"; const="stove0-observation-result/v1" |  |
| `inapplicable` | no | anyOf=#/components/schemas/ObservationInapplicable \| type="null" |  |
| `observer` | yes | #/components/schemas/ObserverImplementation |  |
| `observer_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| `observer_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `request_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `result_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `state` | yes | type="string"; enum=["observed","inapplicable","failed","canceled"] |  |
| `subjects` | yes | type="array"; minItems=1; items=(#/components/schemas/ArtifactSubject) |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | entries | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactSubject](schemas-artifactsubject.md)
- [schemas: JsonSchemaDocument](schemas-jsonschemadocument.md)
- [schemas: JsonValue](schemas-jsonvalue.md)
- [schemas: ObservationFailure](schemas-observationfailure.md)
- [schemas: ObservationInapplicable](schemas-observationinapplicable.md)
- [schemas: ObserverImplementation](schemas-observerimplementation.md)

## Governing policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ObservationResult`

### Exact owned JSON

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
