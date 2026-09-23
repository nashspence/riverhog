# schemas: JoinWorkMemberBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-joinworkmemberbinding:6f7b2083a0 -->

Exact successful branch result used to derive one join work identity.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-c52b2a82ff"></a>

- <a id="s-c68f77fdd2"></a>`type`: `"object"`
- <a id="s-1ac6fc71be"></a>`additionalProperties`: `false`
- <a id="s-7046b4db9c"></a>`description`: `"Exact successful branch result used to derive one join work identity."`
- <a id="s-646ccdb6cd"></a>`required`: `["branch_id","settlement_sha256","artifact_selection_sha256"]`
- <a id="s-6fb692513a"></a>`title`: `"JoinWorkMemberBinding"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e94c9d3dc7"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Artifact Selection Sha256" |  |
| <a id="s-ee37562d56"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-d66dcc0a37"></a>`producer_settlement_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; title="Producer Settlement Sha256" |  |
| <a id="s-6c249ece18"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Settlement Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifact_selection_sha256](#s-e94c9d3dc7) | `length · characters · fixed` | shared above |
| <a id="s-bd4348dafb"></a>[field producer_settlement_sha256 · string value](#s-d66dcc0a37) | `length · characters · fixed` | shared above |
| [field settlement_sha256](#s-6c249ece18) | `length · characters · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-bae4607df2"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-c251992978"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/JoinWorkMemberBinding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ca1974eb0e54c7992f9e43d9e9cd6d0b2bd7ed6455ed768a74efc0a64061735c -->

```json
{
  "additionalProperties": false,
  "description": "Exact successful branch result used to derive one join work identity.",
  "properties": {
    "artifact_selection_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Artifact Selection Sha256",
      "type": "string"
    },
    "branch_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Branch Id",
      "type": "string"
    },
    "producer_settlement_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Producer Settlement Sha256"
    },
    "settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Settlement Sha256",
      "type": "string"
    }
  },
  "required": [
    "branch_id",
    "settlement_sha256",
    "artifact_selection_sha256"
  ],
  "title": "JoinWorkMemberBinding",
  "type": "object"
}
```

</details>
