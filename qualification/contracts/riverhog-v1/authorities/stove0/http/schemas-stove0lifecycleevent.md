# schemas: Stove0LifecycleEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-stove0lifecycleevent:31fdc50ad8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-63a02222a0"></a>
| Field | Shape |
|---|---|
| <a id="s-9d9bc62db0"></a>`discriminator` | additional keys=`mapping`, `propertyName` |
| <a id="s-8f85ecfde2"></a>`oneOf` | items=#/components/schemas/WorkCreatedEvent \| #/components/schemas/WorkUpdatedEvent \| #/components/schemas/BranchSetAdmittedEvent \| #/components/schemas/JoinAdmittedEvent \| #/components/schemas/EvaluationCreatedEvent \| #/components/schemas/EvaluationUpdatedEvent |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: BranchSetAdmittedEvent](schemas-branchsetadmittedevent.md)
- [schemas: EvaluationCreatedEvent](schemas-evaluationcreatedevent.md)
- [schemas: EvaluationUpdatedEvent](schemas-evaluationupdatedevent.md)
- [schemas: JoinAdmittedEvent](schemas-joinadmittedevent.md)
- [schemas: WorkCreatedEvent](schemas-workcreatedevent.md)
- [schemas: WorkUpdatedEvent](schemas-workupdatedevent.md)

## Governing policies

- <a id="pa-8a7759e13e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/Stove0LifecycleEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9dd3a7f5b95c308928d539c76b611464def73897f79ea4d3881961f238c06a9b -->

```json
{
  "discriminator": {
    "mapping": {
      "io.riverhog.stove0.branch-set.admitted": "#/components/schemas/BranchSetAdmittedEvent",
      "io.riverhog.stove0.evaluation.created": "#/components/schemas/EvaluationCreatedEvent",
      "io.riverhog.stove0.evaluation.updated": "#/components/schemas/EvaluationUpdatedEvent",
      "io.riverhog.stove0.join.admitted": "#/components/schemas/JoinAdmittedEvent",
      "io.riverhog.stove0.work.created": "#/components/schemas/WorkCreatedEvent",
      "io.riverhog.stove0.work.updated": "#/components/schemas/WorkUpdatedEvent"
    },
    "propertyName": "type"
  },
  "oneOf": [
    {
      "$ref": "#/components/schemas/WorkCreatedEvent"
    },
    {
      "$ref": "#/components/schemas/WorkUpdatedEvent"
    },
    {
      "$ref": "#/components/schemas/BranchSetAdmittedEvent"
    },
    {
      "$ref": "#/components/schemas/JoinAdmittedEvent"
    },
    {
      "$ref": "#/components/schemas/EvaluationCreatedEvent"
    },
    {
      "$ref": "#/components/schemas/EvaluationUpdatedEvent"
    }
  ]
}
```
