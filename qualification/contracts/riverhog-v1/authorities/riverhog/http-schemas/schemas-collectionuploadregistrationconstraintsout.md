# schemas: CollectionUploadRegistrationConstraintsOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadregistrationconstraintsout:d12bec0aab -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-f33fdf46d9"></a>

- <a id="s-34c81b8c37"></a>`type`: `"object"`
- <a id="s-198118cea1"></a>`additionalProperties`: `false`
- <a id="s-8353943a1b"></a>`required`: `["pack_member_bytes","raw_part_plaintext_bytes"]`
- <a id="s-ec74161340"></a>`title`: `"CollectionUploadRegistrationConstraintsOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-50f00cd230"></a>`pack_member_bytes` | yes | type="integer"; minimum=1; title="Pack Member Bytes" |  |
| <a id="s-7a8fa45dda"></a>`raw_part_plaintext_bytes` | yes | type="integer"; minimum=65536; multipleOf=65536; title="Raw Part Plaintext Bytes" |  |

## Governing policies

- <a id="pa-1c67416503"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadRegistrationConstraintsOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
