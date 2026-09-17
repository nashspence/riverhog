# schemas: ObservationEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-observationevidence:ecea12eb1e -->

Complete routing evidence: immutable request plus accepted result.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-3347a377a9"></a>

- <a id="s-f4c15ef0ea"></a>`type`: `"object"`
- <a id="s-6ef6aa89be"></a>`additionalProperties`: `false`
- <a id="s-798664f8c6"></a>`description`: `"Complete routing evidence: immutable request plus accepted result."`
- <a id="s-8055eab617"></a>`required`: `["request","result"]`
- <a id="s-3b5c248e8e"></a>`title`: `"ObservationEvidence"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2641593ab7"></a>`request` | yes | [ObservationRequest](schemas-observationrequest.md) |  |
| <a id="s-189fcd75d8"></a>`result` | yes | [ObservationResult](schemas-observationresult.md) |  |

## Maintained corroboration

### Referenced contract elements

- [ObservationRequest](schemas-observationrequest.md)
- [ObservationResult](schemas-observationresult.md)

## Governing policies

- <a id="pa-5959a346c5"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ObservationEvidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
