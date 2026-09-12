# schemas: DiscardCollectionUploadRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-discardcollectionuploadrequest:6d32480092 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-8f24d030c0"></a>
- <a id="s-c390fe52ba"></a>`title`: DiscardCollectionUploadRequest
- <a id="s-8b77b67f57"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-60f0a9dd51"></a>`challenge` | yes | type="string"; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$" |  |

## Governing policies

- <a id="pa-5216e727fc"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/DiscardCollectionUploadRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d79783a6821604c4992d7a97f1007b46d65c25bc3f650494e5c4d29412ee3b5d -->

```json
{
  "additionalProperties": false,
  "properties": {
    "challenge": {
      "minLength": 1,
      "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
      "title": "Challenge",
      "type": "string"
    }
  },
  "required": [
    "challenge"
  ],
  "title": "DiscardCollectionUploadRequest",
  "type": "object"
}
```
