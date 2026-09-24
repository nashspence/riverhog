# schemas: SchedulerRun

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-schedulerrun:af455c2ec3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-6407a73b91"></a>

- <a id="s-76621ef99a"></a>`type`: `"object"`
- <a id="s-9a2d1bc1d3"></a>`additionalProperties`: `false`
- <a id="s-f890af8706"></a>`required`: `["pruning","work"]`
- <a id="s-7b437e479d"></a>`title`: `"SchedulerRun"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-42061fffc2"></a>`admission` | no | anyOf=[([AdmissionRun](schemas-admissionrun.md)); (type="null")] |  |
| <a id="s-e949e7bfc6"></a>`pruning` | yes | anyOf=[([SchedulerPruning](schemas-schedulerpruning.md)); (type="null")] |  |
| <a id="s-3b50dd5ead"></a>`work` | yes | [SchedulerWorkBatch](schemas-schedulerworkbatch.md) |  |

## Maintained corroboration

### Referenced contract elements

- [AdmissionRun](schemas-admissionrun.md)
- [SchedulerPruning](schemas-schedulerpruning.md)
- [SchedulerWorkBatch](schemas-schedulerworkbatch.md)

## Governing policies

- <a id="pa-62f9efe8a1"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/SchedulerRun`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
