# schemas: CanonicalRelPath

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-canonicalrelpath:712bce03bd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-e74ba753c4"></a>

- <a id="s-b3e5d56f4f"></a>`type`: `"string"`
- <a id="s-de4ca879e7"></a>`format`: `"riverhog-canonical-relpath-v1"`
- <a id="s-e4873d9a9a"></a>`maxLength`: `4096`
- <a id="s-041cae012f"></a>`minLength`: `1`
- <a id="s-c8e3a51b5b"></a>`pattern`: `"^[^/\\\\]+(?:/[^/\\\\]+)*$"`
- <a id="s-c43503ebca"></a>`x-unicode-normalization`: `"NFC"`

### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-161ce9f1ad"></a>1 | not=(pattern="(?:^\|/)\\.{1,2}(?:/\|$)") |
| <a id="s-3a21cd534f"></a>2 | not=(pattern="^\\s\|\\s$") |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=4096; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [schemas: CanonicalRelPath](#s-e74ba753c4) | `length · characters · contract_max` | shared above |

## Governing policies

- <a id="pa-18bbc517aa"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-fd6c556d2f"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CanonicalRelPath`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fa3392977e23c3c604f49fed3024ddfc514d329289c0f292c3ebe3e4c3748e66 -->

```json
{
  "allOf": [
    {
      "not": {
        "pattern": "(?:^|/)\\.{1,2}(?:/|$)"
      }
    },
    {
      "not": {
        "pattern": "^\\s|\\s$"
      }
    }
  ],
  "format": "riverhog-canonical-relpath-v1",
  "maxLength": 4096,
  "minLength": 1,
  "pattern": "^[^/\\\\]+(?:/[^/\\\\]+)*$",
  "type": "string",
  "x-unicode-normalization": "NFC"
}
```

</details>
