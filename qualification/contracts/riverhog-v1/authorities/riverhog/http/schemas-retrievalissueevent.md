# schemas: RetrievalIssueEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-retrievalissueevent:e15379040f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-39ba081052a8"></a>
- <a id="s-c9688ae39e24"></a>`title`: RetrievalIssueEvent
- <a id="s-b499cfd26053"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ed3ecdd6cc0d"></a>`data` | yes | #/components/schemas/RetrievalIssueData |  |
| <a id="s-a30d8d6a0119"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-4395d57bb8db"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-90d4c9a8c710"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-415f6d1727ac"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-840c8c2eb511"></a>`subject` | no | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-63d40366dbaf"></a>`time` | yes | type="string" |  |
| <a id="s-7346fa5b7164"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.issue" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: RetrievalIssueData](schemas-retrievalissuedata.md)

## Governing policies

- <a id="pa-9f08bbc6e8a8"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalIssueEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6a47a3eb99b6baa5403973df94bb32789973c413ad2a08f90804ed2b7da7a404 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/RetrievalIssueData"
    },
    "datacontenttype": {
      "const": "application/json",
      "default": "application/json",
      "title": "Datacontenttype",
      "type": "string"
    },
    "id": {
      "minLength": 1,
      "title": "Id",
      "type": "string"
    },
    "source": {
      "minLength": 1,
      "title": "Source",
      "type": "string"
    },
    "specversion": {
      "const": "1.0",
      "default": "1.0",
      "title": "Specversion",
      "type": "string"
    },
    "subject": {
      "anyOf": [
        {
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Subject"
    },
    "time": {
      "title": "Time",
      "type": "string"
    },
    "type": {
      "const": "io.riverhog.riverhog.retrieval.issue",
      "title": "Type",
      "type": "string"
    }
  },
  "required": [
    "id",
    "source",
    "type",
    "time",
    "data"
  ],
  "title": "RetrievalIssueEvent",
  "type": "object"
}
```
