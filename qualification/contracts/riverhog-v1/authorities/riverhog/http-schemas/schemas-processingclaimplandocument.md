# schemas: ProcessingClaimPlanDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-processingclaimplandocument:20e57e73e4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-834a2c4308"></a>

- <a id="s-90ca763aba"></a>`type`: `"object"`
- <a id="s-3f6811f473"></a>`additionalProperties`: `false`
- `if`: [See `if`](#s-1114014bd6)
- <a id="s-c34a098626"></a>`required`: `["execution_id","controller_evidence","controller_evidence_sha256","operation","inputs","artifacts","source_collection_retirement_policy","source_collection_retirement_grace_seconds","sealed_at"]`
- `then`: [See `then`](#s-00a7ae18fe)
- <a id="s-f8ed83976a"></a>`title`: `"ProcessingClaimPlanDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a2e1e51a8f"></a>`artifacts` | yes | [ArtifactSetIdentityDocument](schemas-artifactsetidentitydocument.md) |  |
| <a id="s-c31b744472"></a>`controller_evidence` | yes | type="object"; additionalProperties=(any JSON value); title="Controller Evidence"; x-riverhog-encoded-bytes-max=16777216; x-riverhog-extent={"policy":"contract_max","reason":"bounded-controller-evidence-envelope"} |  |
| <a id="s-827ae4b73c"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Controller Evidence Sha256" |  |
| <a id="s-68f89f7550"></a>`execution_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Id" |  |
| <a id="s-6449700174"></a>`inputs` | yes | [ExactSetIdentityDocument](schemas-exactsetidentitydocument.md) |  |
| <a id="s-b014f9416d"></a>`operation` | yes | [OperationIdentityDocument](schemas-operationidentitydocument.md) |  |
| <a id="s-0c58a348ae"></a>`sealed_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Sealed At" |  |
| <a id="s-b1be02b077"></a>`source_collection_retirement_grace_seconds` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=0 |  |
| <a id="s-c27282ca8a"></a>`source_collection_retirement_policy` | yes | type="string"; enum=["retain","retire-after-verified-output"]; title="Source Collection Retirement Policy" |  |

### <a id="s-1114014bd6"></a>`if`


#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eb4c211748"></a>`source_collection_retirement_policy` | no | const="retain" |  |

### <a id="s-00a7ae18fe"></a>`then`


#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b5c09b8f05"></a>`source_collection_retirement_grace_seconds` | no | const="0" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field controller_evidence](#s-c31b744472) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field controller_evidence](#s-c31b744472) | `encoded-size · bytes · contract_max` | maximum=16777216; reason="bounded-controller-evidence-envelope"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field controller_evidence_sha256](#s-827ae4b73c) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field execution_id](#s-68f89f7550) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field sealed_at](#s-0c58a348ae) | `length · characters · fixed` | maximum=30; minimum=30; reason="fixed-public-representation" |

## Maintained corroboration

### Referenced contract elements

- [ArtifactSetIdentityDocument](schemas-artifactsetidentitydocument.md)
- [ExactSetIdentityDocument](schemas-exactsetidentitydocument.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)
- [OperationIdentityDocument](schemas-operationidentitydocument.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-89cf546390"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-d4c1676bb7"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-b3e3917ee3"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimPlanDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de0bbb4a58b2d51a7e332659cdde0cfa9bb9ffc350e50379fa2981274f9b9297 -->

```json
{
  "additionalProperties": false,
  "if": {
    "properties": {
      "source_collection_retirement_policy": {
        "const": "retain"
      }
    }
  },
  "properties": {
    "artifacts": {
      "$ref": "#/components/schemas/ArtifactSetIdentityDocument"
    },
    "controller_evidence": {
      "additionalProperties": true,
      "title": "Controller Evidence",
      "type": "object",
      "x-riverhog-encoded-bytes-max": 16777216,
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-controller-evidence-envelope"
      }
    },
    "controller_evidence_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Controller Evidence Sha256",
      "type": "string"
    },
    "execution_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Execution Id",
      "type": "string"
    },
    "inputs": {
      "$ref": "#/components/schemas/ExactSetIdentityDocument"
    },
    "operation": {
      "$ref": "#/components/schemas/OperationIdentityDocument"
    },
    "sealed_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Sealed At",
      "type": "string"
    },
    "source_collection_retirement_grace_seconds": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 0
    },
    "source_collection_retirement_policy": {
      "enum": [
        "retain",
        "retire-after-verified-output"
      ],
      "title": "Source Collection Retirement Policy",
      "type": "string"
    }
  },
  "required": [
    "execution_id",
    "controller_evidence",
    "controller_evidence_sha256",
    "operation",
    "inputs",
    "artifacts",
    "source_collection_retirement_policy",
    "source_collection_retirement_grace_seconds",
    "sealed_at"
  ],
  "then": {
    "properties": {
      "source_collection_retirement_grace_seconds": {
        "const": "0"
      }
    }
  },
  "title": "ProcessingClaimPlanDocument",
  "type": "object"
}
```

</details>
