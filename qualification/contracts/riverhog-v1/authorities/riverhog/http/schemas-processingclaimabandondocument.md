# schemas: ProcessingClaimAbandonDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-processingclaimabandondocument:a22f22a1c6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-77a4d0e1e5"></a>
- <a id="s-1d4fc7bd0f"></a>`title`: ProcessingClaimAbandonDocument
- <a id="s-f5816b0081"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eb811339a3"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-334fdcb37c"></a>`reason` | yes | type="string"; minLength=1; maxLength=1000 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1000; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field reason](#s-334fdcb37c) | `length · characters · contract_max` | shared above |

## Governing policies

- <a id="pa-0a1d9becca"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-6a89f16079"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimAbandonDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: db2136a2c3b36883b975822f17c927e5ac2386f19c8c8c8fa80ac47af99be5f0 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "reason": {
      "maxLength": 1000,
      "minLength": 1,
      "title": "Reason",
      "type": "string"
    }
  },
  "required": [
    "fence",
    "reason"
  ],
  "title": "ProcessingClaimAbandonDocument",
  "type": "object"
}
```
