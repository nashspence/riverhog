# schemas: ProcessingClaimPlanSealDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-processingclaimplansealdocument:262b450351 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-6b1eb68537"></a>

- <a id="s-bb18e4de0b"></a>`type`: `"object"`
- <a id="s-aae25016c5"></a>`additionalProperties`: `false`
- `if`: [See `if`](#s-f21f1cdca7)
- <a id="s-0f04b946cc"></a>`required`: `["fence","execution_id","controller_evidence","controller_evidence_sha256","operation"]`
- `then`: [See `then`](#s-a9fd6219ca)
- <a id="s-3646380164"></a>`title`: `"ProcessingClaimPlanSealDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bbf8920793"></a>`controller_evidence` | yes | type="object"; additionalProperties=(any JSON value); title="Controller Evidence"; x-riverhog-encoded-bytes-max=16777216; x-riverhog-extent={"policy":"contract_max","reason":"bounded-controller-evidence-envelope"} |  |
| <a id="s-2b58360cd0"></a>`controller_evidence_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Controller Evidence Sha256" |  |
| <a id="s-d28fb67370"></a>`execution_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Execution Id" |  |
| <a id="s-58a14c2ad6"></a>`fence` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-36b7aad560"></a>`operation` | yes | [OperationIdentityDocument](schemas-operationidentitydocument.md) |  |
| <a id="s-b211349c51"></a>`source_collection_retirement_grace_seconds` | no | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=0 |  |
| <a id="s-2047d360b2"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain"; title="Source Collection Retirement Policy" | Retain source collections, or permit their permanent deletion after verified output, the grace period, and collection deletion checks. |

### <a id="s-f21f1cdca7"></a>`if`


#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fe03f3024e"></a>`source_collection_retirement_policy` | no | const="retain" |  |

### <a id="s-a9fd6219ca"></a>`then`


#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2cc124ab6c"></a>`source_collection_retirement_grace_seconds` | no | const="0" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field controller_evidence](#s-bbf8920793) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field controller_evidence](#s-bbf8920793) | `encoded-size · bytes · contract_max` | maximum=16777216; reason="bounded-controller-evidence-envelope"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field controller_evidence_sha256](#s-2b58360cd0) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field execution_id](#s-d28fb67370) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract elements

- [NonnegativeDecimal](schemas-nonnegativedecimal.md)
- [OperationIdentityDocument](schemas-operationidentitydocument.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-eff461e713"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-b07a73d5aa"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-93764b00d4"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimPlanSealDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e486d60158cc21ae09ef5aa2535f467d1ddbb763dd7bd2c48f20489836e0b33d -->

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
    "fence": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 1
    },
    "operation": {
      "$ref": "#/components/schemas/OperationIdentityDocument"
    },
    "source_collection_retirement_grace_seconds": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 0
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
    }
  },
  "required": [
    "fence",
    "execution_id",
    "controller_evidence",
    "controller_evidence_sha256",
    "operation"
  ],
  "then": {
    "properties": {
      "source_collection_retirement_grace_seconds": {
        "const": "0"
      }
    }
  },
  "title": "ProcessingClaimPlanSealDocument",
  "type": "object"
}
```

</details>
