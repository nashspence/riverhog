# schemas: CollectionTag

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectiontag:bc15f44db5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-c3ed7114f2"></a>

- <a id="s-3c1043523b"></a>`type`: `"string"`
- <a id="s-3e4c7e330b"></a>`maxLength`: `65536`
- <a id="s-208c2993c7"></a>`minLength`: `1`
- <a id="s-7828e57af4"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-0bd7cb3b7d"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-60363d629f"></a>`x-unicode-normalization`: `"NFC"`

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=65536

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [schemas: CollectionTag](#s-c3ed7114f2) | `encoded-size · bytes · contract_max` | reason="bounded-human-authored-collection-tag"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [schemas: CollectionTag](#s-c3ed7114f2) | `length · characters · contract_max` | minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-e9bfcb474d"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-295b6b39e8"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionTag`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b6917e633cfcae1991f58fecc90c7c91406e442bcbfea53917a973300b49dc7 -->

```json
{
  "maxLength": 65536,
  "minLength": 1,
  "type": "string",
  "x-riverhog-encoded-bytes-max": 65536,
  "x-riverhog-extent": {
    "policy": "contract_max",
    "reason": "bounded-human-authored-collection-tag"
  },
  "x-unicode-normalization": "NFC"
}
```

</details>
