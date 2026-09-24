# schemas: CoordinationChildSettlementRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-coordinationchildsettlementref:0cf64d6302 -->

Exact direct-child success included in a coordination settlement.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-f6790286f8"></a>

- <a id="s-c278867e11"></a>`type`: `"object"`
- <a id="s-b392bc8dcb"></a>`additionalProperties`: `false`
- <a id="s-6a3dd80955"></a>`description`: `"Exact direct-child success included in a coordination settlement."`
- <a id="s-497ade4f16"></a>`required`: `["branch_id","kind","settlement_sha256"]`
- <a id="s-331df79f8a"></a>`title`: `"CoordinationChildSettlementRef"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d752442b7f"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-bcaee0be02"></a>`kind` | yes | type="string"; enum=["collection","external-effect","coordination"]; title="Kind" |  |
| <a id="s-2bf9c1c0c2"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Settlement Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field settlement_sha256](#s-2bf9c1c0c2) | `length · characters · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-ef0e05b6f1"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-e83e6c9607"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/CoordinationChildSettlementRef`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7bd31023cc09897af3321f85d7438ce508e1093e2abf51326d29362ab02bb869 -->

```json
{
  "additionalProperties": false,
  "description": "Exact direct-child success included in a coordination settlement.",
  "properties": {
    "branch_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Branch Id",
      "type": "string"
    },
    "kind": {
      "enum": [
        "collection",
        "external-effect",
        "coordination"
      ],
      "title": "Kind",
      "type": "string"
    },
    "settlement_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Settlement Sha256",
      "type": "string"
    }
  },
  "required": [
    "branch_id",
    "kind",
    "settlement_sha256"
  ],
  "title": "CoordinationChildSettlementRef",
  "type": "object"
}
```

</details>
