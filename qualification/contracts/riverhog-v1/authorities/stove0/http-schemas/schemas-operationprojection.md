# schemas: OperationProjection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-operationprojection:d5b4e64ab9 -->

One declarative JSON-pointer copy into an operation request.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-d57d52f12b"></a>

- <a id="s-976351d405"></a>`type`: `"object"`
- <a id="s-2b77f4794d"></a>`additionalProperties`: `false`
- <a id="s-94c32e17af"></a>`description`: `"One declarative JSON-pointer copy into an operation request."`
- <a id="s-9fa2a69c9e"></a>`required`: `["source","source_pointer","destination","destination_pointer"]`
- <a id="s-2c1cc9b409"></a>`title`: `"OperationProjection"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0b1200ab83"></a>`destination` | yes | type="string"; enum=["intent","target-options"]; title="Destination" |  |
| <a id="s-dc538e63b4"></a>`destination_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Destination Pointer" |  |
| <a id="s-e5a6d9d1b9"></a>`source` | yes | type="string"; enum=["work-effective-intent","work-evaluation"]; title="Source" |  |
| <a id="s-10cb05929b"></a>`source_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$"; title="Source Pointer" |  |

## Governing policies

- <a id="pa-b1fb0fcd2f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/OperationProjection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 412cc6fe371170daf347f525d51353e79834de6222531002457e205e033c56fa -->

```json
{
  "additionalProperties": false,
  "description": "One declarative JSON-pointer copy into an operation request.",
  "properties": {
    "destination": {
      "enum": [
        "intent",
        "target-options"
      ],
      "title": "Destination",
      "type": "string"
    },
    "destination_pointer": {
      "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
      "title": "Destination Pointer",
      "type": "string"
    },
    "source": {
      "enum": [
        "work-effective-intent",
        "work-evaluation"
      ],
      "title": "Source",
      "type": "string"
    },
    "source_pointer": {
      "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
      "title": "Source Pointer",
      "type": "string"
    }
  },
  "required": [
    "source",
    "source_pointer",
    "destination",
    "destination_pointer"
  ],
  "title": "OperationProjection",
  "type": "object"
}
```

</details>
