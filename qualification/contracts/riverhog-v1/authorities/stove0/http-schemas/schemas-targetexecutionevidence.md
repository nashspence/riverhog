# schemas: TargetExecutionEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-targetexecutionevidence:e63f30feef -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-d1cc74145f"></a>

- <a id="s-a1556263da"></a>`type`: `"object"`
- <a id="s-37e15c18d3"></a>`additionalProperties`: `false`
- <a id="s-7d03e268f9"></a>`required`: `["target_descriptor_sha256","operation_contract_sha256","plan_sha256","execution_sha256"]`
- <a id="s-1b9cbf8489"></a>`title`: `"TargetExecutionEvidence"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0702eb4e69"></a>`execution_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Sha256" |  |
| <a id="s-190d671a90"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Operation Contract Sha256" |  |
| <a id="s-427d486dae"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Plan Sha256" |  |
| <a id="s-a67d9da8a7"></a>`runtime` | no | type="object"; additionalProperties=([JsonValue](schemas-jsonvalue.md)); title="Runtime" |  |
| <a id="s-075f3c5f75"></a>`target_descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Descriptor Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field runtime](#s-a67d9da8a7) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field execution_sha256](#s-0702eb4e69) | `length · characters · fixed` | shared above |
| [field operation_contract_sha256](#s-190d671a90) | `length · characters · fixed` | shared above |
| [field plan_sha256](#s-427d486dae) | `length · characters · fixed` | shared above |
| [field target_descriptor_sha256](#s-075f3c5f75) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [JsonValue](schemas-jsonvalue.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-98d927c932"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-e0bacb9601"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-2bfb34468a"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetExecutionEvidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 648ada196be329112eda1bb96672428d3af4428290cf094128403a9fe1a3a169 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "execution_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Execution Sha256",
      "type": "string"
    },
    "operation_contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Operation Contract Sha256",
      "type": "string"
    },
    "plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Plan Sha256",
      "type": "string"
    },
    "runtime": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Runtime",
      "type": "object"
    },
    "target_descriptor_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Target Descriptor Sha256",
      "type": "string"
    }
  },
  "required": [
    "target_descriptor_sha256",
    "operation_contract_sha256",
    "plan_sha256",
    "execution_sha256"
  ],
  "title": "TargetExecutionEvidence",
  "type": "object"
}
```

</details>
