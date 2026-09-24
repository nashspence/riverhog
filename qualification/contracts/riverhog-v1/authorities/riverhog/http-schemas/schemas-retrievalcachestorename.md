# schemas: RetrievalCacheStoreName

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retrievalcachestorename:78d4a58a5c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-db5681696a"></a>

- <a id="s-4d3683a277"></a>`type`: `"string"`
- <a id="s-3fa5453383"></a>`pattern`: `"^[a-z0-9]+(?:-[a-z0-9]+)*$"`

## Governing policies

- <a id="pa-a88954e33a"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetrievalCacheStoreName`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d6b95c0f6d2f7eb930ce727b7fbcdfadb27eefc2677befeeaba2cd88b965f2f -->

```json
{
  "pattern": "^[a-z0-9]+(?:-[a-z0-9]+)*$",
  "type": "string"
}
```

</details>
