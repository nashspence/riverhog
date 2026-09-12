# schemas: PreviewAcceptanceView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-previewacceptanceview:1921dbce59 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-e57385ebd5"></a>
- <a id="s-f8886fc573"></a>`title`: PreviewAcceptanceView
- <a id="s-2c142acf40"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c6efe4c6c0"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-887084af69"></a>`preview_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-dd225aebd6"></a>`target_plans` | yes | type="array"; items=(#/components/schemas/PreviewTargetExpectationView) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field target_plans](#s-dd225aebd6) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field branch_set_sha256](#s-c6efe4c6c0) | `length · characters · fixed` | shared above |
| [field preview_sha256](#s-887084af69) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: PreviewTargetExpectationView](schemas-previewtargetexpectationview.md)

## Governing policies

- <a id="pa-e0e3eeaddd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-9e9566d724"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-2cac4a613a"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/PreviewAcceptanceView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e903a5a8059f4c069db940e9fbb02a433f66d3d048c897077e9e35dcac7e90d7 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "branch_set_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Branch Set Sha256",
      "type": "string"
    },
    "preview_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Preview Sha256",
      "type": "string"
    },
    "target_plans": {
      "items": {
        "$ref": "#/components/schemas/PreviewTargetExpectationView"
      },
      "title": "Target Plans",
      "type": "array"
    }
  },
  "required": [
    "preview_sha256",
    "branch_set_sha256",
    "target_plans"
  ],
  "title": "PreviewAcceptanceView",
  "type": "object"
}
```
