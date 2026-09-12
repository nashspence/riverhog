# schemas: CollectionDescription

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiondescription:12d7c3bc08 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-6a9620fec4"></a>
- <a id="s-e2824971b9"></a>`type`: string

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=32768

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [schemas: CollectionDescription](#s-6a9620fec4) | `encoded-size · bytes · contract_max` | reason="bounded-human-authored-catalog-description"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [schemas: CollectionDescription](#s-6a9620fec4) | `length · characters · contract_max` | minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-936e995d95"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-27e30ebe5e"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDescription`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: daefae928b8ba365eb083c697fe3d6d1635c9772e1689e8785f4b8c33183cf9a -->

```json
{
  "maxLength": 32768,
  "minLength": 1,
  "type": "string",
  "x-riverhog-encoded-bytes-max": 32768,
  "x-riverhog-extent": {
    "policy": "contract_max",
    "reason": "bounded-human-authored-catalog-description"
  },
  "x-unicode-normalization": "NFC"
}
```
