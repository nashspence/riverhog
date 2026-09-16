# schemas: ApplicationAccessSort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-applicationaccesssort:bd884f481e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-bda7a7250b"></a>

- <a id="s-11068b284c"></a>`type`: `"string"`
- <a id="s-967b58a780"></a>`enum`: `["app","key_id","permission","resource","created_at"]`

## Governing policies

- <a id="pa-72eb555e1a"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ApplicationAccessSort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0731760f4bd8d1c00707979b4059c00713fe218169fd52654846d46a568f5ad6 -->

```json
{
  "enum": [
    "app",
    "key_id",
    "permission",
    "resource",
    "created_at"
  ],
  "type": "string"
}
```

</details>
