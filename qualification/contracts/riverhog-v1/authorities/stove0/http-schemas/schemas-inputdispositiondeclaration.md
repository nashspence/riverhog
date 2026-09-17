# schemas: InputDispositionDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-inputdispositiondeclaration:77ab436abb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-4ddd41618e"></a>

- <a id="s-3adace929e"></a>`type`: `"object"`
- <a id="s-d53b2388ec"></a>`additionalProperties`: `false`
- <a id="s-d96c0db3c3"></a>`required`: `["input_id","status"]`
- <a id="s-d5ad6d5824"></a>`title`: `"InputDispositionDeclaration"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3bebb08faf"></a>`input_id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Input Id" |  |
| <a id="s-b1127018b8"></a>`status` | yes | type="string"; enum=["transformed","preserved","omitted","rejected"]; title="Status" |  |

## Governing policies

- <a id="pa-6f3e7c8e9e"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/InputDispositionDeclaration`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e94d6588a913858886bdbd76c5ea8b089aeca0bc91025c491822f3a23edf6dba -->

```json
{
  "additionalProperties": false,
  "properties": {
    "input_id": {
      "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
      "title": "Input Id",
      "type": "string"
    },
    "status": {
      "enum": [
        "transformed",
        "preserved",
        "omitted",
        "rejected"
      ],
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "input_id",
    "status"
  ],
  "title": "InputDispositionDeclaration",
  "type": "object"
}
```

</details>
