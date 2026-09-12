# schemas: BranchPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-branchplan:1a290228b3 -->

One named required child work using the ordinary WorkflowPlan contract.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5396b7fe22"></a>
- <a id="s-22c75a0a68"></a>`title`: BranchPlan
- <a id="s-bd87915606"></a>`description`: One named required child work using the ordinary WorkflowPlan contract.
- <a id="s-2ce2e459f5"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2cdbafca54"></a>`artifact_selection` | yes | #/components/schemas/ArtifactSelectionRef |  |
| <a id="s-160b934f9b"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2a843b5db3"></a>`kind` | no | type="string"; const="leaf" |  |
| <a id="s-2dd1c57657"></a>`workflow_plan` | yes | #/components/schemas/WorkflowPlan |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactSelectionRef](schemas-artifactselectionref.md)
- [schemas: WorkflowPlan](schemas-workflowplan.md)

## Governing policies

- <a id="pa-7f24619e4e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchPlan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39986da6d0d0d70636814ba3e4f631cef339fe0c8a681f1f52df62051ce36bc2 -->

```json
{
  "additionalProperties": false,
  "description": "One named required child work using the ordinary WorkflowPlan contract.",
  "properties": {
    "artifact_selection": {
      "$ref": "#/components/schemas/ArtifactSelectionRef"
    },
    "branch_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Branch Id",
      "type": "string"
    },
    "kind": {
      "const": "leaf",
      "default": "leaf",
      "title": "Kind",
      "type": "string"
    },
    "workflow_plan": {
      "$ref": "#/components/schemas/WorkflowPlan"
    }
  },
  "required": [
    "branch_id",
    "artifact_selection",
    "workflow_plan"
  ],
  "title": "BranchPlan",
  "type": "object"
}
```
