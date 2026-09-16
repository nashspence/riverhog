# schemas: SearchSort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-searchsort:dc4f330eeb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-5b1c61b0b9"></a>

- <a id="s-56fc7d4b94"></a>`type`: `"string"`
- <a id="s-a761c1da99"></a>`enum`: `["file_ref","collection_id","path","bytes"]`

## Governing policies

- <a id="pa-37f3ec8211"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/SearchSort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c6fde4040d73ef203ee97bc475dee3403109d98e39a4bb7e46a61b02b8be1e9c -->

```json
{
  "enum": [
    "file_ref",
    "collection_id",
    "path",
    "bytes"
  ],
  "type": "string"
}
```

</details>
