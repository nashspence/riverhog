# schemas: TargetCallbackAcknowledgement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-targetcallbackacknowledgement:ddcd27b594 -->

Idempotent acceptance of one execution-scoped declaration.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-4bf66cf0d2"></a>

- <a id="s-6654fe9b23"></a>`type`: `"object"`
- <a id="s-a91f67873c"></a>`additionalProperties`: `false`
- <a id="s-b30e7fdc9a"></a>`description`: `"Idempotent acceptance of one execution-scoped declaration."`
- <a id="s-fcda9a7a6d"></a>`title`: `"TargetCallbackAcknowledgement"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3be75837a3"></a>`accepted` | no | type="boolean"; const=true; default=true; title="Accepted" |  |

## Governing policies

- <a id="pa-777690247c"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetCallbackAcknowledgement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
