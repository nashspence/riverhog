# schemas: ApplicationResource

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-applicationresource:e2740bcf9f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-08a55fd79a"></a>

- <a id="s-fb44b61e34"></a>`type`: `"string"`
- <a id="s-84f2be9938"></a>`pattern`: `"^(?:\\*\|tag:.+\|collection:[1-9][0-9]*)$"`

## Governing policies

- <a id="pa-23552b2e95"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ApplicationResource`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af986ec94e5a3a25f56abfe26f255df17d8fd2b3abf7ae7225dde4cbf06d9a30 -->

```json
{
  "pattern": "^(?:\\*|tag:.+|collection:[1-9][0-9]*)$",
  "type": "string"
}
```

</details>
