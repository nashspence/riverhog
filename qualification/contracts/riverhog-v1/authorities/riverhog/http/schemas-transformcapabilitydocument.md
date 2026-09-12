# schemas: TransformCapabilityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-transformcapabilitydocument:bb0aec854b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

- `title`: TransformCapabilityDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `actions` | yes | type="array"; minItems=1; items=(type="string"; enum=["read-inputs","write-output"]); oneOf=const=["read-inputs"] \| const=["read-inputs","write-output"] |  |
| `artifacts` | yes | #/components/schemas/ArtifactReceivingSetDocument |  |
| `audience` | yes | type="string"; pattern="^[a-z0-9][a-z0-9._:/-]{0,299}$" |  |
| `claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `expires_at` | yes | type="string"; minLength=1; maxLength=64 |  |
| `fence` | yes | type="integer"; minimum=1 |  |
| `format` | yes | type="string"; const="riverhog-transform-capability/v1" |  |
| `id` | yes | type="string"; minLength=1; maxLength=160 |  |
| `principal_app` | yes | type="string"; minLength=1; maxLength=300 |  |
| `state` | yes | type="string"; enum=["receiving","active"] |  |
| `token` | yes | type="string"; pattern="^rhc_[A-Za-z0-9_-]+$" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=300, minimum=1, reason=schema-maximum |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactReceivingSetDocument](schemas-artifactreceivingsetdocument.md)

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
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/TransformCapabilityDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 17e910223e622a78026a55a52e0d8af93f39c5ed7c175b4459bc71c7dc707c83 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "actions": {
      "items": {
        "enum": [
          "read-inputs",
          "write-output"
        ],
        "type": "string"
      },
      "minItems": 1,
      "oneOf": [
        {
          "const": [
            "read-inputs"
          ]
        },
        {
          "const": [
            "read-inputs",
            "write-output"
          ]
        }
      ],
      "title": "Actions",
      "type": "array"
    },
    "artifacts": {
      "$ref": "#/components/schemas/ArtifactReceivingSetDocument"
    },
    "audience": {
      "pattern": "^[a-z0-9][a-z0-9._:/-]{0,299}$",
      "title": "Audience",
      "type": "string"
    },
    "claim_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Claim Id",
      "type": "string"
    },
    "expires_at": {
      "maxLength": 64,
      "minLength": 1,
      "title": "Expires At",
      "type": "string"
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "format": {
      "const": "riverhog-transform-capability/v1",
      "title": "Format",
      "type": "string"
    },
    "id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Id",
      "type": "string"
    },
    "principal_app": {
      "maxLength": 300,
      "minLength": 1,
      "title": "Principal App",
      "type": "string"
    },
    "state": {
      "enum": [
        "receiving",
        "active"
      ],
      "title": "State",
      "type": "string"
    },
    "token": {
      "pattern": "^rhc_[A-Za-z0-9_-]+$",
      "title": "Token",
      "type": "string"
    }
  },
  "required": [
    "format",
    "id",
    "claim_id",
    "fence",
    "audience",
    "actions",
    "state",
    "principal_app",
    "expires_at",
    "artifacts",
    "token"
  ],
  "title": "TransformCapabilityDocument",
  "type": "object"
}
```
