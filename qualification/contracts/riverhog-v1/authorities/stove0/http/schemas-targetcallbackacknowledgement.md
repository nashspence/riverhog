# schemas: TargetCallbackAcknowledgement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetcallbackacknowledgement:6df5ac040c -->

Idempotent acceptance of one execution-scoped declaration.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-4bf66cf0d22f"></a>
- <a id="s-fcda9a7a6d12"></a>`title`: TargetCallbackAcknowledgement
- <a id="s-b30e7fdc9a4a"></a>`description`: Idempotent acceptance of one execution-scoped declaration.
- <a id="s-6654fe9b2337"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3be75837a338"></a>`accepted` | no | type="boolean"; const=true |  |

## Governing policies

- <a id="pa-e8d35059c5c2"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetCallbackAcknowledgement`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 888aeb6259ebbfb946fb389f43530f2e7f42b0aaa47127fc93cde9b46c06ccfc -->

```json
{
  "additionalProperties": false,
  "description": "Idempotent acceptance of one execution-scoped declaration.",
  "properties": {
    "accepted": {
      "const": true,
      "default": true,
      "title": "Accepted",
      "type": "boolean"
    }
  },
  "title": "TargetCallbackAcknowledgement",
  "type": "object"
}
```
