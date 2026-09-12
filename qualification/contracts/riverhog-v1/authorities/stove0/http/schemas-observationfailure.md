# schemas: ObservationFailure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-observationfailure:adf294519a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-10ed2a22d3"></a>
- <a id="s-2a1e3e6ff7"></a>`title`: ObservationFailure
- <a id="s-f21d520fcd"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-780bca4cc5"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-cdaaaeb75e"></a>`message` | yes | type="string"; minLength=1; maxLength=1000 |  |
| <a id="s-ba1e2539ef"></a>`retryable` | yes | type="boolean" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1000; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field message](#s-cdaaaeb75e) | `length · characters · contract_max` | shared above |

## Governing policies

- <a id="pa-e595138747"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-9f149bd38e"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ObservationFailure`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 03452b85273ec047f313b026f245c633c5677b66c3c74316258933f63c051c7f -->

```json
{
  "additionalProperties": false,
  "properties": {
    "code": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Code",
      "type": "string"
    },
    "message": {
      "maxLength": 1000,
      "minLength": 1,
      "title": "Message",
      "type": "string"
    },
    "retryable": {
      "title": "Retryable",
      "type": "boolean"
    }
  },
  "required": [
    "code",
    "message",
    "retryable"
  ],
  "title": "ObservationFailure",
  "type": "object"
}
```
