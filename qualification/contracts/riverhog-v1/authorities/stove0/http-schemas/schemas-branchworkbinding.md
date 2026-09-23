# schemas: BranchWorkBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-branchworkbinding:670233b06e -->

Stable parent/branch lineage for one ordinary child work identity.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-f7dcdb0c66"></a>

- <a id="s-6505930055"></a>`type`: `"object"`
- <a id="s-6958836cc8"></a>`additionalProperties`: `false`
- <a id="s-357ebc62a4"></a>`description`: `"Stable parent/branch lineage for one ordinary child work identity."`
- <a id="s-d93bca70c7"></a>`required`: `["parent_work_id","branch_id","decision_sha256","artifact_selection_sha256"]`
- <a id="s-353bffec5d"></a>`title`: `"BranchWorkBinding"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7218be4aaa"></a>`artifact_selection_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Artifact Selection Sha256" |  |
| <a id="s-c8d54a9890"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Branch Id" |  |
| <a id="s-7c7260eac9"></a>`decision_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Decision Sha256" |  |
| <a id="s-1924d622ee"></a>`kind` | no | type="string"; const="branch"; default="branch"; title="Kind" |  |
| <a id="s-7a56e70881"></a>`parent_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Parent Work Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifact_selection_sha256](#s-7218be4aaa) | `length · characters · fixed` | shared above |
| [field decision_sha256](#s-7c7260eac9) | `length · characters · fixed` | shared above |
| [field parent_work_id](#s-7a56e70881) | `length · characters · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-b70a821c0a"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-69dd3e8095"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchWorkBinding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0c3297729fbc53bc25d0dcb1bf587a91b7991848f5584c8248180f28b506ecd9 -->

```json
{
  "additionalProperties": false,
  "description": "Stable parent/branch lineage for one ordinary child work identity.",
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
    "decision_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Decision Sha256",
      "type": "string"
    },
    "kind": {
      "const": "branch",
      "default": "branch",
      "title": "Kind",
      "type": "string"
    },
    "parent_work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Parent Work Id",
      "type": "string"
    }
  },
  "required": [
    "parent_work_id",
    "branch_id",
    "decision_sha256",
    "artifact_selection_sha256"
  ],
  "title": "BranchWorkBinding",
  "type": "object"
}
```

</details>
