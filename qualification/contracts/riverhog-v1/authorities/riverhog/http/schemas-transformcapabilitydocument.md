# schemas: TransformCapabilityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-transformcapabilitydocument:bb0aec854b -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/TransformCapabilityDocument`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactReceivingSetDocument](schemas-artifactreceivingsetdocument.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=64, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=160, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=300, minimum=1, reason=schema-maximum |

## Contract summary

- `title`: TransformCapabilityDocument
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `actions` | yes | array |  |
| `artifacts` | yes | #/components/schemas/ArtifactReceivingSetDocument |  |
| `audience` | yes | string |  |
| `claim_id` | yes | string |  |
| `expires_at` | yes | string |  |
| `fence` | yes | integer |  |
| `format` | yes | string |  |
| `id` | yes | string |  |
| `principal_app` | yes | string |  |
| `state` | yes | string |  |
| `token` | yes | string |  |

## Complete owned contract

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
