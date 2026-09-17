# schemas: ApplicationKeySort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-applicationkeysort:79402766c7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-935dcd2931"></a>

- <a id="s-a14d0b704e"></a>`type`: `"string"`
- <a id="s-b4fe8a169a"></a>`enum`: `["id","created_at","expires_at","last_used_at"]`

## Governing policies

- <a id="pa-cdc9ae39da"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ApplicationKeySort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c85d6a69b8004d0d26f7b8e2f6c9b239b122879feedebc40fdb5ab1eb265d882 -->

```json
{
  "enum": [
    "id",
    "created_at",
    "expires_at",
    "last_used_at"
  ],
  "type": "string"
}
```

</details>
