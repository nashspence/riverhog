# schemas: WorkCreatedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-workcreatedeventdata:072476601e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-9af115887b"></a>
- <a id="s-df8a9cdcd8"></a>`title`: WorkCreatedEventData
- <a id="s-f8a826eb7c"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6c02a77812"></a>`branch_set_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-bf555aadc7"></a>`join_plan_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-ede34ccc3c"></a>`parent_work_id` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-c800d03ab0"></a>`phase` | yes | type="string"; enum=["eligible","claimed","observing","planning","target_preflight","queued","executing","output_finalizing","verifying","settled","retirement_pending","coordinating","abandon_pending","complete","inapplicable","failed","canceled"] |  |
| <a id="s-b531fc61e2"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-92a6fd3a4c"></a>[field branch_set_sha256 · string value](#s-6c02a77812) | `length · characters · fixed` | shared above |
| <a id="s-6ba2d826a4"></a>[field join_plan_sha256 · string value](#s-bf555aadc7) | `length · characters · fixed` | shared above |
| <a id="s-844c82cb1d"></a>[field parent_work_id · string value](#s-ede34ccc3c) | `length · characters · fixed` | shared above |
| [field work_id](#s-b531fc61e2) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-22130b3dd4"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-ad45c30d87"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/WorkCreatedEventData`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 15e73ba78808d3cbf0eb37d99e952a1899d1266f2ae521f01c1865583f936b0c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "branch_set_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Branch Set Sha256"
    },
    "join_plan_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Join Plan Sha256"
    },
    "parent_work_id": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Parent Work Id"
    },
    "phase": {
      "enum": [
        "eligible",
        "claimed",
        "observing",
        "planning",
        "target_preflight",
        "queued",
        "executing",
        "output_finalizing",
        "verifying",
        "settled",
        "retirement_pending",
        "coordinating",
        "abandon_pending",
        "complete",
        "inapplicable",
        "failed",
        "canceled"
      ],
      "title": "Phase",
      "type": "string"
    },
    "work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Id",
      "type": "string"
    }
  },
  "required": [
    "work_id",
    "phase"
  ],
  "title": "WorkCreatedEventData",
  "type": "object"
}
```
