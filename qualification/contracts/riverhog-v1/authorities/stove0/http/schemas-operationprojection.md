# schemas: OperationProjection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-operationprojection:ef6a8f5504 -->

One declarative JSON-pointer copy into an operation request.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d57d52f12bdc"></a>
- <a id="s-2c1cc9b409a9"></a>`title`: OperationProjection
- <a id="s-94c32e17afd1"></a>`description`: One declarative JSON-pointer copy into an operation request.
- <a id="s-976351d405be"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0b1200ab8397"></a>`destination` | yes | type="string"; enum=["intent","target-options"] |  |
| <a id="s-dc538e63b488"></a>`destination_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-e5a6d9d1b908"></a>`source` | yes | type="string"; enum=["work-effective-intent","work-evaluation"] |  |
| <a id="s-10cb05929b39"></a>`source_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

## Governing policies

- <a id="pa-35c0bfbc1bab"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/OperationProjection`

### Exact owned JSON

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
