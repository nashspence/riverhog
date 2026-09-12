# schemas: CompleteCollectionUploadCustodyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-completecollectionuploadcustodyout:05ed40534c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-932782fc0c71"></a>
- <a id="s-8d3905c96ddb"></a>`title`: CompleteCollectionUploadCustodyOut
- <a id="s-17bb2349dae1"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-070a0e2319a2"></a>`state` | yes | type="string"; const="complete" |  |

## Governing policies

- <a id="pa-2d0d5e6c12fc"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CompleteCollectionUploadCustodyOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e2710a21f095aac00ba34d113f099bded56cbe528cf70ce4f2f25eb490f5b758 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "state": {
      "const": "complete",
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "state"
  ],
  "title": "CompleteCollectionUploadCustodyOut",
  "type": "object"
}
```
