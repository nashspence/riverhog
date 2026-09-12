# schemas: ErrorResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog-ftp-adapter:schemas-errorresponse:934af2ff90 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](index.md#f-2f7960c106) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b6d05ff815"></a>
- <a id="s-d67d8f58d8"></a>`title`: ErrorResponse
- <a id="s-f5613aa2f2"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-836e8d89ec"></a>`error` | yes | #/components/schemas/ErrorBody |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ErrorBody](schemas-errorbody.md)

## Governing policies

- <a id="pa-92a57d656a"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog-ftp-adapter](../../../evidence/sources.md#src-c3a51ac29a) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/components/schemas/ErrorResponse`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f512af6aea84106cb490abc5a849a56c6836aec507f2a2906f6d3301ed551137 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "error": {
      "$ref": "#/components/schemas/ErrorBody"
    }
  },
  "required": [
    "error"
  ],
  "title": "ErrorResponse",
  "type": "object"
}
```
