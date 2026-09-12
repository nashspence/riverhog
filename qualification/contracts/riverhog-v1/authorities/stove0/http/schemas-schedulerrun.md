# schemas: SchedulerRun

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-schedulerrun:389a6dbea7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6407a73b9125"></a>
- <a id="s-7b437e479dc4"></a>`title`: SchedulerRun
- <a id="s-76621ef99a4e"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-42061fffc266"></a>`admission` | no | anyOf=#/components/schemas/AdmissionRun \| type="null" |  |
| <a id="s-e949e7bfc61a"></a>`pruning` | yes | anyOf=#/components/schemas/SchedulerPruning \| type="null" |  |
| <a id="s-3b50dd5ead53"></a>`work` | yes | #/components/schemas/SchedulerWorkBatch |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: AdmissionRun](schemas-admissionrun.md)
- [schemas: SchedulerPruning](schemas-schedulerpruning.md)
- [schemas: SchedulerWorkBatch](schemas-schedulerworkbatch.md)

## Governing policies

- <a id="pa-27416b0531d5"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/SchedulerRun`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 18340e39255b5eeffda73125a891665cd47056ed52aae5e771c92c75a8d1cac8 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "admission": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/AdmissionRun"
        },
        {
          "type": "null"
        }
      ]
    },
    "pruning": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/SchedulerPruning"
        },
        {
          "type": "null"
        }
      ]
    },
    "work": {
      "$ref": "#/components/schemas/SchedulerWorkBatch"
    }
  },
  "required": [
    "pruning",
    "work"
  ],
  "title": "SchedulerRun",
  "type": "object"
}
```
