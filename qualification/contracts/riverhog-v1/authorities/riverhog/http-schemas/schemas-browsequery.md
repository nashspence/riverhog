# schemas: BrowseQuery

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-browsequery:4c499c8090 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-5e7782ca79"></a>

- <a id="s-c21e23758b"></a>`type`: `"string"`
- <a id="s-7676c6d276"></a>`maxLength`: `4096`
- <a id="s-615a7f4f4d"></a>`minLength`: `1`
- <a id="s-7abfe81797"></a>`pattern`: `"^\\S(?:[\\s\\S]*\\S)?$"`

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=4096; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [schemas: BrowseQuery](#s-5e7782ca79) | `length · characters · contract_max` | shared above |

## Governing policies

- <a id="pa-019169d963"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-5b0b06bd3d"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/BrowseQuery`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 602c72fd196e5a6dcf80f03cb263d0c08cd81dc7587ae34af8973f95d8cfe9c6 -->

```json
{
  "maxLength": 4096,
  "minLength": 1,
  "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
  "type": "string"
}
```

</details>
