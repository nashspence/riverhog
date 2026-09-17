# schemas: BrowseQuery

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-browsequery:b14b4cf2d5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-23b87a3a3f"></a>

- <a id="s-1ddb8144cb"></a>`type`: `"string"`
- <a id="s-9bbcd81ac8"></a>`maxLength`: `4096`
- <a id="s-436c9baa16"></a>`minLength`: `1`
- <a id="s-1e8d6c9995"></a>`pattern`: `"^\\S(?:[\\s\\S]*\\S)?$"`

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=4096; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [schemas: BrowseQuery](#s-23b87a3a3f) | `length · characters · contract_max` | shared above |

## Governing policies

- <a id="pa-6573b4511e"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-44e614c577"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BrowseQuery`

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
