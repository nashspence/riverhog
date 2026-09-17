# schemas: FactPredicate

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-factpredicate:fbfdd64056 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-22c7204e70"></a>

- <a id="s-76c251dc87"></a>`type`: `"object"`
- <a id="s-bdf011cc1b"></a>`additionalProperties`: `false`
- <a id="s-3d35fb5fd5"></a>`required`: `["observation_contract_id","pointer"]`
- <a id="s-be3396ece7"></a>`title`: `"FactPredicate"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-faaa06076f"></a>`artifact_facts` | no | anyOf=[([ArtifactFactBinding](schemas-artifactfactbinding.md)); (type="null")] |  |
| <a id="s-31625c701a"></a>`artifact_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); title="Artifact Roles" |  |
| <a id="s-0220713d42"></a>`observation_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Observation Contract Id" |  |
| <a id="s-6bd68de14c"></a>`operator` | no | type="string"; enum=["equals","not-equals","contains","exists"]; default="equals"; title="Operator" |  |
| <a id="s-8a0d6e7ba5"></a>`pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Pointer" |  |
| <a id="s-72f5b5f698"></a>`value` | no | [JsonValue](schemas-jsonvalue.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifact_roles](#s-31625c701a) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [ArtifactFactBinding](schemas-artifactfactbinding.md)
- [JsonValue](schemas-jsonvalue.md)

## Governing policies

- <a id="pa-b5d65d7526"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-bbbf93ebd2"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/FactPredicate`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
