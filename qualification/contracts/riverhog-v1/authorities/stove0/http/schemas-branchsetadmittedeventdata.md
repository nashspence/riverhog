# schemas: BranchSetAdmittedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-branchsetadmittedeventdata:96986e89f7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-16d5f32e9321"></a>
- <a id="s-a1cabb9566e9"></a>`title`: BranchSetAdmittedEventData
- <a id="s-5dfeea34edb8"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-049468e4f236"></a>`admitted_work_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-a2fd0e638ae7"></a>`branch_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-544a85591795"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-232dc582e41b"></a>`phase` | yes | type="string"; const="coordinating" |  |
| <a id="s-825795c0a5bc"></a>`revision` | yes | type="integer"; minimum=2 |  |
| <a id="s-cea2f2c1d215"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field branch_set_sha256](#s-544a85591795) | `length · characters · fixed` | shared above |
| [field work_id](#s-cea2f2c1d215) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-b47b1c98b2fa"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-aa7954c0938b"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchSetAdmittedEventData`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8915aca1dade95650ccfb03c5ac47cf18fb3cd693a2c2becad387ba7624688f6 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "admitted_work_count": {
      "minimum": 1,
      "title": "Admitted Work Count",
      "type": "integer"
    },
    "branch_count": {
      "minimum": 1,
      "title": "Branch Count",
      "type": "integer"
    },
    "branch_set_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Branch Set Sha256",
      "type": "string"
    },
    "phase": {
      "const": "coordinating",
      "title": "Phase",
      "type": "string"
    },
    "revision": {
      "minimum": 2,
      "title": "Revision",
      "type": "integer"
    },
    "work_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Work Id",
      "type": "string"
    }
  },
  "required": [
    "work_id",
    "phase",
    "revision",
    "branch_set_sha256",
    "branch_count",
    "admitted_work_count"
  ],
  "title": "BranchSetAdmittedEventData",
  "type": "object"
}
```
