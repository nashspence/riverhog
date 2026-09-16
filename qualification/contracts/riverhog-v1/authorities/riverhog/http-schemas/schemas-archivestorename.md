# schemas: ArchiveStoreName

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivestorename:e48bc1398a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-f791d51b96"></a>

- <a id="s-6f146d8d7b"></a>`type`: `"string"`
- <a id="s-a12c4b83aa"></a>`pattern`: `"^[a-z0-9]+(?:-[a-z0-9]+)*$"`

## Governing policies

- <a id="pa-655cfa3625"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveStoreName`

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
