# schemas: FactPredicate

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-factpredicate:fa55463c7e -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 1 |

## Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/FactPredicate`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArtifactFactBinding](schemas-artifactfactbinding.md)
- [schemas: JsonValue](schemas-jsonvalue.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `title`: FactPredicate
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `artifact_facts` | no | object (1 fields) |  |
| `artifact_roles` | no | array |  |
| `observation_contract_id` | yes | string |  |
| `operator` | no | string |  |
| `pointer` | yes | string |  |
| `value` | no | #/components/schemas/JsonValue |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a3fbdfea1363baf2c5aa6809fcc1319e37898bcc5e18a0856484bc7903596b4a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "artifact_facts": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArtifactFactBinding"
        },
        {
          "type": "null"
        }
      ]
    },
    "artifact_roles": {
      "default": [],
      "items": {
        "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
        "type": "string"
      },
      "title": "Artifact Roles",
      "type": "array"
    },
    "observation_contract_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Observation Contract Id",
      "type": "string"
    },
    "operator": {
      "default": "equals",
      "enum": [
        "equals",
        "not-equals",
        "contains",
        "exists"
      ],
      "title": "Operator",
      "type": "string"
    },
    "pointer": {
      "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
      "title": "Pointer",
      "type": "string"
    },
    "value": {
      "$ref": "#/components/schemas/JsonValue"
    }
  },
  "required": [
    "observation_contract_id",
    "pointer"
  ],
  "title": "FactPredicate",
  "type": "object"
}
```
