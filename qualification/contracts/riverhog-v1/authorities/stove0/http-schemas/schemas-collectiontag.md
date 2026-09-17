# schemas: CollectionTag

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-collectiontag:02012c8d16 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-cb959d5e83"></a>

- <a id="s-5f87a1f231"></a>`type`: `"string"`
- <a id="s-2f2dcf2da5"></a>`maxLength`: `65536`
- <a id="s-1e24fa68d5"></a>`minLength`: `1`
- <a id="s-fda88ade70"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-e34452ce07"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-06a3bb0d31"></a>`x-unicode-normalization`: `"NFC"`

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=65536

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [schemas: CollectionTag](#s-cb959d5e83) | `encoded-size · bytes · contract_max` | reason="bounded-human-authored-collection-tag"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [schemas: CollectionTag](#s-cb959d5e83) | `length · characters · contract_max` | minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-5c8aab70ec"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-4208372b63"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/CollectionTag`

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
