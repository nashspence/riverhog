# schemas: RenewRetrievalJobRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-renewretrievaljobrequest:9184554874 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-93df6fe639"></a>
- <a id="s-2aece4cd02"></a>`title`: RenewRetrievalJobRequest
- <a id="s-4aa08d60e7"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-22f144242f"></a>`lease_seconds` | yes | type="integer"; minimum=1 |  |

## Governing policies

- <a id="pa-51ad55d3eb"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RenewRetrievalJobRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c340f9b632598fe59aae3fc711a245ed0211b36c94b1b056c43b0a5bc95e419b -->

```json
{
  "additionalProperties": false,
  "properties": {
    "lease_seconds": {
      "minimum": 1,
      "title": "Lease Seconds",
      "type": "integer"
    }
  },
  "required": [
    "lease_seconds"
  ],
  "title": "RenewRetrievalJobRequest",
  "type": "object"
}
```
