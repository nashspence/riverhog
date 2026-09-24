# schemas: BranchSetAdmittedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-branchsetadmittedeventdata:79a598f654 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-16d5f32e93"></a>

- <a id="s-5dfeea34ed"></a>`type`: `"object"`
- <a id="s-e3f41df5d7"></a>`additionalProperties`: `false`
- <a id="s-b56eb13f7e"></a>`required`: `["work_id","phase","revision","branch_set_sha256","branch_count","admitted_work_count"]`
- <a id="s-a1cabb9566"></a>`title`: `"BranchSetAdmittedEventData"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-049468e4f2"></a>`admitted_work_count` | yes | type="integer"; minimum=1; title="Admitted Work Count" |  |
| <a id="s-a2fd0e638a"></a>`branch_count` | yes | type="integer"; minimum=1; title="Branch Count" |  |
| <a id="s-544a855917"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Branch Set Sha256" |  |
| <a id="s-232dc582e4"></a>`phase` | yes | type="string"; const="coordinating"; title="Phase" |  |
| <a id="s-825795c0a5"></a>`revision` | yes | type="integer"; minimum=2; title="Revision" |  |
| <a id="s-cea2f2c1d2"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Work Id" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field branch_set_sha256](#s-544a855917) | `length · characters · fixed` | shared above |
| [field work_id](#s-cea2f2c1d2) | `length · characters · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-1cafc29ac5"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-838149f91c"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchSetAdmittedEventData`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
