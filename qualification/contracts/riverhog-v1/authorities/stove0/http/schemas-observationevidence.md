# schemas: ObservationEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-observationevidence:7cfc081818 -->

Complete routing evidence: immutable request plus accepted result.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3347a377a9fc"></a>
- <a id="s-3b5c248e8ec0"></a>`title`: ObservationEvidence
- <a id="s-798664f8c6be"></a>`description`: Complete routing evidence: immutable request plus accepted result.
- <a id="s-f4c15ef0eaeb"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2641593ab722"></a>`request` | yes | #/components/schemas/ObservationRequest |  |
| <a id="s-189fcd75d8da"></a>`result` | yes | #/components/schemas/ObservationResult |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ObservationRequest](schemas-observationrequest.md)
- [schemas: ObservationResult](schemas-observationresult.md)

## Governing policies

- <a id="pa-faf210666935"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ObservationEvidence`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a06d5d9e675fcc32ff369ef0d620be72d9a5f4c330e1025f97210e12985be6cc -->

```json
{
  "additionalProperties": false,
  "description": "Complete routing evidence: immutable request plus accepted result.",
  "properties": {
    "request": {
      "$ref": "#/components/schemas/ObservationRequest"
    },
    "result": {
      "$ref": "#/components/schemas/ObservationResult"
    }
  },
  "required": [
    "request",
    "result"
  ],
  "title": "ObservationEvidence",
  "type": "object"
}
```
