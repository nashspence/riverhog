# schemas: CollectionTag

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-collectiontag:ab6981dc7e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-cb959d5e83"></a>
- <a id="s-5f87a1f231"></a>`type`: string

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=65536

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [schemas: CollectionTag](#s-cb959d5e83) | `encoded-size · bytes · contract_max` | reason="bounded-human-authored-collection-tag"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [schemas: CollectionTag](#s-cb959d5e83) | `length · characters · contract_max` | minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-822ca206b3"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-899247475e"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/CollectionTag`

### Exact owned JSON

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
