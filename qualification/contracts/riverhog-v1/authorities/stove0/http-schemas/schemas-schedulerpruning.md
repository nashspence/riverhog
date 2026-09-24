# schemas: SchedulerPruning

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-schedulerpruning:45ad67d8db -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-f4823c7029"></a>

- <a id="s-d359ab4d29"></a>`type`: `"object"`
- <a id="s-770918e1e5"></a>`additionalProperties`: `false`
- <a id="s-484b92a447"></a>`required`: `["work","work_bytes","evaluations","evaluation_bytes","selections","selection_bytes","events","event_bytes"]`
- <a id="s-463735ae7c"></a>`title`: `"SchedulerPruning"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8b859e9f63"></a>`evaluation_bytes` | yes | type="integer"; minimum=0; title="Evaluation Bytes" |  |
| <a id="s-fd976e6001"></a>`evaluations` | yes | type="integer"; minimum=0; title="Evaluations" |  |
| <a id="s-3448818870"></a>`event_bytes` | yes | type="integer"; minimum=0; title="Event Bytes" |  |
| <a id="s-09489dff04"></a>`events` | yes | type="integer"; minimum=0; title="Events" |  |
| <a id="s-dd591fce04"></a>`selection_bytes` | yes | type="integer"; minimum=0; title="Selection Bytes" |  |
| <a id="s-9addac7c48"></a>`selections` | yes | type="integer"; minimum=0; title="Selections" |  |
| <a id="s-9cf0537268"></a>`work` | yes | type="integer"; minimum=0; title="Work" |  |
| <a id="s-2445982f0c"></a>`work_bytes` | yes | type="integer"; minimum=0; title="Work Bytes" |  |

## Governing policies

- <a id="pa-1c5134a65e"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/SchedulerPruning`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 639614e3a2443f84fe63c2477757b5c11d526671cd5d37014cf1699993cccc8c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "evaluation_bytes": {
      "minimum": 0,
      "title": "Evaluation Bytes",
      "type": "integer"
    },
    "evaluations": {
      "minimum": 0,
      "title": "Evaluations",
      "type": "integer"
    },
    "event_bytes": {
      "minimum": 0,
      "title": "Event Bytes",
      "type": "integer"
    },
    "events": {
      "minimum": 0,
      "title": "Events",
      "type": "integer"
    },
    "selection_bytes": {
      "minimum": 0,
      "title": "Selection Bytes",
      "type": "integer"
    },
    "selections": {
      "minimum": 0,
      "title": "Selections",
      "type": "integer"
    },
    "work": {
      "minimum": 0,
      "title": "Work",
      "type": "integer"
    },
    "work_bytes": {
      "minimum": 0,
      "title": "Work Bytes",
      "type": "integer"
    }
  },
  "required": [
    "work",
    "work_bytes",
    "evaluations",
    "evaluation_bytes",
    "selections",
    "selection_bytes",
    "events",
    "event_bytes"
  ],
  "title": "SchedulerPruning",
  "type": "object"
}
```

</details>
