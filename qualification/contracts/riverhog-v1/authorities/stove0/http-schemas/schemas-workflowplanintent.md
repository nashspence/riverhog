# schemas: WorkflowPlanIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-workflowplanintent:328b666c34 -->

Work-independent fields that deterministically materialize a workflow plan.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-7e51d1430d"></a>

- <a id="s-642b2b2763"></a>`type`: `"object"`
- <a id="s-eb3f8e8eb0"></a>`additionalProperties`: `false`
- <a id="s-d32c77c568"></a>`description`: `"Work-independent fields that deterministically materialize a workflow plan."`
- <a id="s-a43fe15478"></a>`required`: `["operation","target_registration_id","target_descriptor_sha256"]`
- <a id="s-23c8e189de"></a>`title`: `"WorkflowPlanIntent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fc12db4ac7"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only"; title="Input Retrieval Policy" |  |
| <a id="s-dec7652768"></a>`operation` | yes | [OperationRef](schemas-operationref.md) |  |
| <a id="s-99bb65af48"></a>`output_policy` | no | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Output Policy" |  |
| <a id="s-0bb17d7fff"></a>`requested_target_options` | no | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Requested Target Options" |  |
| <a id="s-37b4b89175"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection"; title="Result Kind" |  |
| <a id="s-7df81052bc"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0; title="Source Collection Retirement Grace Seconds" |  |
| <a id="s-c535b4dedf"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain"; title="Source Collection Retirement Policy" | Retain source collections, or permit their permanent deletion after verified output, the grace period, and collection deletion checks. |
| <a id="s-8dbb70f238"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |
| <a id="s-cabd6d9c4e"></a>`target_registration_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9.-]{0,118}[a-z0-9])?$"; title="Target Registration Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field output_policy](#s-99bb65af48) | `cardinality · entries · operational_policy` | shared above |
| [field requested_target_options](#s-0bb17d7fff) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field target_descriptor_sha256](#s-8dbb70f238) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [JsonValue](schemas-jsonvalue.md)
- [OperationRef](schemas-operationref.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-afe97fc901"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-6baa6dfffe"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-abcc685753"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkflowPlanIntent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6c1fc9c01d7137f700b13db7122bd84690aa92d1135822f4b26800503ae11b12 -->

```json
{
  "additionalProperties": false,
  "description": "Work-independent fields that deterministically materialize a workflow plan.",
  "properties": {
    "input_retrieval_policy": {
      "default": "available-only",
      "enum": [
        "available-only",
        "allow"
      ],
      "title": "Input Retrieval Policy",
      "type": "string"
    },
    "operation": {
      "$ref": "#/components/schemas/OperationRef"
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
    }
  },
  "required": [
    "operation",
    "target_registration_id",
    "target_descriptor_sha256"
  ],
  "title": "WorkflowPlanIntent",
  "type": "object"
}
```

</details>
