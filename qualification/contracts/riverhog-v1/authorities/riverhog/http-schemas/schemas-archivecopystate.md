# schemas: ArchiveCopyState

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopystate:6d1476ce8f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-d113767fbf"></a>

- <a id="s-6f785dbf45"></a>`type`: `"string"`
- <a id="s-5420044d1b"></a>`enum`: `["requested","waiting","checking","copying","canceling","completed","failed","canceled"]`

## Governing policies

- <a id="pa-286137961d"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyState`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fef2e66a0ed103e056d143a56b935cdc4bf167947bf6809ceaa7da45027e7d18 -->

```json
{
  "enum": [
    "requested",
    "waiting",
    "checking",
    "copying",
    "canceling",
    "completed",
    "failed",
    "canceled"
  ],
  "type": "string"
}
```

</details>
