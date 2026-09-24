# schemas: OutputSourceEdge

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-outputsourceedge:0b5d5f22eb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-899c649fd5"></a>

- <a id="s-176e844f07"></a>`type`: `"object"`
- <a id="s-f722a43250"></a>`additionalProperties`: `false`
- <a id="s-8b17613715"></a>`required`: `["output_id","input_id"]`
- <a id="s-ac563c5a8c"></a>`title`: `"OutputSourceEdge"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-52c77a58c0"></a>`input_id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Input Id" |  |
| <a id="s-6c7b5cd2e8"></a>`output_id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Output Id" |  |

## Governing policies

- <a id="pa-ec678d4aa7"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/OutputSourceEdge`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 972b082324ce99560dcf290aaa2190dca0d8d4ad38ed3ae820655588e94db2f4 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "input_id": {
      "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
      "title": "Input Id",
      "type": "string"
    },
    "output_id": {
      "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
      "title": "Output Id",
      "type": "string"
    }
  },
  "required": [
    "output_id",
    "input_id"
  ],
  "title": "OutputSourceEdge",
  "type": "object"
}
```

</details>
