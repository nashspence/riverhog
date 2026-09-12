# schemas: FactPredicate

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-factpredicate:fa55463c7e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-22c7204e70d7"></a>
- <a id="s-be3396ece7c5"></a>`title`: FactPredicate
- <a id="s-76c251dc8785"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-faaa06076fd3"></a>`artifact_facts` | no | anyOf=#/components/schemas/ArtifactFactBinding \| type="null" |  |
| <a id="s-31625c701a6b"></a>`artifact_roles` | no | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-0220713d427d"></a>`observation_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-6bd68de14ce2"></a>`operator` | no | type="string"; enum=["equals","not-equals","contains","exists"] |  |
| <a id="s-8a0d6e7ba568"></a>`pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-72f5b5f6980a"></a>`value` | no | #/components/schemas/JsonValue |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifact_roles](#s-31625c701a6b) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactFactBinding](schemas-artifactfactbinding.md)
- [schemas: JsonValue](schemas-jsonvalue.md)

## Governing policies

- <a id="pa-5e48815d35fb"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-9c6e57e71b3c"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/FactPredicate`

### Exact owned JSON

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
