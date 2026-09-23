# schemas: ContentObservationEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-contentobservationevidence:510f04e0b1 -->

Complete routing evidence: immutable request plus accepted result.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-4b7b7d569b"></a>

- <a id="s-c0c301b837"></a>`type`: `"object"`
- <a id="s-1e36ad9230"></a>`additionalProperties`: `false`
- <a id="s-32422513f3"></a>`description`: `"Complete routing evidence: immutable request plus accepted result."`
- <a id="s-fd5593134e"></a>`required`: `["request","result"]`
- <a id="s-c28f43bebf"></a>`title`: `"ContentObservationEvidence"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d2d543226b"></a>`request` | yes | [ContentObservationRequest](schemas-contentobservationrequest.md) |  |
| <a id="s-f5705ad8db"></a>`result` | yes | [ContentObservationResult](schemas-contentobservationresult.md) |  |

## Maintained corroboration

### Referenced contract elements

- [ContentObservationRequest](schemas-contentobservationrequest.md)
- [ContentObservationResult](schemas-contentobservationresult.md)

## Governing policies

- <a id="pa-80147b0509"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ContentObservationEvidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 426a3add478d67f7ff6c70c2b778fa5c217187fcd02431358847a781e3b3d491 -->

```json
{
  "additionalProperties": false,
  "description": "Complete routing evidence: immutable request plus accepted result.",
  "properties": {
    "request": {
      "$ref": "#/components/schemas/ContentObservationRequest"
    },
    "result": {
      "$ref": "#/components/schemas/ContentObservationResult"
    }
  },
  "required": [
    "request",
    "result"
  ],
  "title": "ContentObservationEvidence",
  "type": "object"
}
```

</details>
