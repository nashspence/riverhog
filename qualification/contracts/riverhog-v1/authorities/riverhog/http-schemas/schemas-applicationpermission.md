# schemas: ApplicationPermission

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-applicationpermission:7488c87b76 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-7455dc9b56"></a>

- <a id="s-18fcf1e938"></a>`type`: `"string"`
- <a id="s-04647b6fab"></a>`enum`: `["*","catalog:read","retrieval:manage","collections:create","collection-descriptions:manage","collection-transforms:control","collection-transforms:execute","collection-tags:manage","collections:delete","archives:read","archives:manage","keys:manage","quotas:manage","events:read","events:read_all","provenance:read","provenance:export"]`

## Governing policies

- <a id="pa-77baf1cdf7"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ApplicationPermission`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 57f5bd75d2d765c41017200a7e93d1c2d601d563e9e965b28339b25a6a2440eb -->

```json
{
  "enum": [
    "*",
    "catalog:read",
    "retrieval:manage",
    "collections:create",
    "collection-descriptions:manage",
    "collection-transforms:control",
    "collection-transforms:execute",
    "collection-tags:manage",
    "collections:delete",
    "archives:read",
    "archives:manage",
    "keys:manage",
    "quotas:manage",
    "events:read",
    "events:read_all",
    "provenance:read",
    "provenance:export"
  ],
  "type": "string"
}
```

</details>
