# schemas: CanonicalRelPath

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-canonicalrelpath:ba7377c5c3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-e74ba753c4ba"></a>
- <a id="s-b3e5d56f4fe0"></a>`type`: string
- <a id="s-de4ca879e7e7"></a>`format`: riverhog-canonical-relpath-v1

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=4096; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [schemas: CanonicalRelPath](#s-e74ba753c4ba) | `length · characters · contract_max` | shared above |

## Governing policies

- <a id="pa-7ce6016676b1"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-37a68a7d1c13"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CanonicalRelPath`

### Exact owned JSON

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
