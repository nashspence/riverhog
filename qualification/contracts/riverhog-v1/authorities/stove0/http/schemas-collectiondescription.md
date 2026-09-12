# schemas: CollectionDescription

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-collectiondescription:37a982bce1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-a9656ba901bb"></a>
- <a id="s-e92fe0fa378e"></a>`type`: string

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=32768

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [schemas: CollectionDescription](#s-a9656ba901bb) | `encoded-size · bytes · contract_max` | reason="bounded-human-authored-catalog-description"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [schemas: CollectionDescription](#s-a9656ba901bb) | `length · characters · contract_max` | minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-12dc261a8ca1"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-616f1adcf314"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/CollectionDescription`

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
