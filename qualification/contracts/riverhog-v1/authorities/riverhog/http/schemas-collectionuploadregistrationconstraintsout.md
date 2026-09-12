# schemas: CollectionUploadRegistrationConstraintsOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadregistrationconstraintsout:1425405212 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f33fdf46d9"></a>
- <a id="s-ec74161340"></a>`title`: CollectionUploadRegistrationConstraintsOut
- <a id="s-34c81b8c37"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-50f00cd230"></a>`pack_member_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-7a8fa45dda"></a>`raw_part_plaintext_bytes` | yes | type="integer"; minimum=65536; additional keys=`multipleOf` |  |

## Governing policies

- <a id="pa-00d845fe6b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadRegistrationConstraintsOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7aac28c92c49ffe2bfe22febf49889b4b3668893294ddc9413e878b7641b98de -->

```json
{
  "additionalProperties": false,
  "properties": {
    "pack_member_bytes": {
      "minimum": 1,
      "title": "Pack Member Bytes",
      "type": "integer"
    },
    "raw_part_plaintext_bytes": {
      "minimum": 65536,
      "multipleOf": 65536,
      "title": "Raw Part Plaintext Bytes",
      "type": "integer"
    }
  },
  "required": [
    "pack_member_bytes",
    "raw_part_plaintext_bytes"
  ],
  "title": "CollectionUploadRegistrationConstraintsOut",
  "type": "object"
}
```
