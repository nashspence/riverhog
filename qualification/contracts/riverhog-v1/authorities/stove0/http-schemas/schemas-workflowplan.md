# schemas: WorkflowPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-workflowplan:f7a5b96416 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-5da2fc6710"></a>

- <a id="s-c51e7ceed0"></a>`type`: `"object"`
- <a id="s-26ff74a165"></a>`additionalProperties`: `false`
- <a id="s-c34ef6a5be"></a>`required`: `["work","operation","target_registration_id","target_descriptor_sha256","workflow_plan_sha256"]`
- <a id="s-d1846f1c20"></a>`title`: `"WorkflowPlan"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6b03446110"></a>`format` | no | type="string"; const="stove0-workflow-plan/v1"; default="stove0-workflow-plan/v1"; title="Format" |  |
| <a id="s-6ad1a7b068"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Input Retrieval Policy" |  |
| <a id="s-6ef6cec03d"></a>`observations` | no | type="array"; default=[]; items=([ContentObservationEvidence](schemas-contentobservationevidence.md)); title="Observations" |  |
| <a id="s-5cabb9dd3b"></a>`operation` | yes | [OperationIdentityRef](schemas-operationidentityref.md) |  |
| <a id="s-bf4f29710f"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Output Policy" |  |
| <a id="s-a5d912ee09"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Requested Target Options" |  |
| <a id="s-f570ec6b98"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection"; title="Result Kind" |  |
| <a id="s-5458b8c50f"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0; title="Source Collection Retirement Grace Seconds" |  |
| <a id="s-337018905f"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain"; title="Source Collection Retirement Policy" | Retain source collections, or permit their permanent deletion after verified output, the grace period, and collection deletion checks. |
| <a id="s-7a1632338e"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-8e91339dcf"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Target Registration Id" |  |
| <a id="s-28aa8c8fac"></a>`work` | yes | [WorkIdentity](schemas-workidentity.md) |  |
| <a id="s-212a10b34a"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Workflow Plan Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field observations](#s-6ef6cec03d) | `cardinality · items · operational_policy` | shared above |
| [field output_policy](#s-bf4f29710f) | `cardinality · entries · operational_policy` | shared above |
| [field requested_target_options](#s-a5d912ee09) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field target_descriptor_sha256](#s-7a1632338e) | `length · characters · fixed` | shared above |
| [field workflow_plan_sha256](#s-212a10b34a) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ContentObservationEvidence](schemas-contentobservationevidence.md)
- [JsonValue](schemas-jsonvalue.md)
- [OperationIdentityRef](schemas-operationidentityref.md)
- [WorkIdentity](schemas-workidentity.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-44a156b282"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-c901b05556"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-86b6d8f856"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkflowPlan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8c34d73b41aeefad11319bb8c7e5ad643f2a8c32a158b8a8208f638fe2078c43 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "format": {
      "const": "stove0-workflow-plan/v1",
      "default": "stove0-workflow-plan/v1",
      "title": "Format",
      "type": "string"
    },
    "input_retrieval_policy": {
      "default": "available-only",
      "enum": [
        "available-only",
        "allow"
      ],
      "title": "Input Retrieval Policy",
      "type": "string"
    },
    "observations": {
      "default": [],
      "items": {
        "$ref": "#/components/schemas/ContentObservationEvidence"
      },
      "title": "Observations",
      "type": "array"
    },
    "operation": {
      "$ref": "#/components/schemas/OperationIdentityRef"
    },
    "output_policy": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Output Policy",
      "type": "object"
    },
    "requested_target_options": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Requested Target Options",
      "type": "object"
    },
    "result_kind": {
      "default": "collection",
      "enum": [
        "collection",
        "external-effect"
      ],
      "title": "Result Kind",
      "type": "string"
    },
    "source_collection_retirement_grace_seconds": {
      "default": 0,
      "minimum": 0,
      "title": "Source Collection Retirement Grace Seconds",
      "type": "integer"
    },
    "source_collection_retirement_policy": {
      "default": "retain",
      "description": "Retain source collections, or permit their permanent deletion after verified output, the grace period, and collection deletion checks.",
      "enum": [
        "retain",
        "retire-after-verified-output"
      ],
      "title": "Source Collection Retirement Policy",
      "type": "string"
    },
    "target_descriptor_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Target Descriptor Sha256",
      "type": "string"
    },
    "target_registration_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
      "title": "Target Registration Id",
      "type": "string"
    },
    "work": {
      "$ref": "#/components/schemas/WorkIdentity"
    },
    "workflow_plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Workflow Plan Sha256",
      "type": "string"
    }
  },
  "required": [
    "work",
    "operation",
    "target_registration_id",
    "target_descriptor_sha256",
    "workflow_plan_sha256"
  ],
  "title": "WorkflowPlan",
  "type": "object"
}
```

</details>
