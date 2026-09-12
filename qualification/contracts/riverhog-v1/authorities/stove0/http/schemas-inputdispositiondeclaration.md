# schemas: InputDispositionDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-inputdispositiondeclaration:3a76d71343 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-4ddd41618e"></a>
- <a id="s-d5ad6d5824"></a>`title`: InputDispositionDeclaration
- <a id="s-3adace929e"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3bebb08faf"></a>`input_id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$" |  |
| <a id="s-b1127018b8"></a>`status` | yes | type="string"; enum=["transformed","preserved","omitted","rejected"] |  |

## Governing policies

- <a id="pa-600c82f103"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/InputDispositionDeclaration`

### Exact owned JSON

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
