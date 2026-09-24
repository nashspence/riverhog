# schemas: CollectionDescription

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectiondescription:50130a1189 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-6a9620fec4"></a>

- <a id="s-e2824971b9"></a>`type`: `"string"`
- <a id="s-6a4209e363"></a>`maxLength`: `32768`
- <a id="s-ed8de38790"></a>`minLength`: `1`
- <a id="s-8e3ca84d34"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-5a05f07383"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-b5383010bb"></a>`x-unicode-normalization`: `"NFC"`

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=32768

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [schemas: CollectionDescription](#s-6a9620fec4) | `encoded-size · bytes · contract_max` | reason="bounded-human-authored-catalog-description"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [schemas: CollectionDescription](#s-6a9620fec4) | `length · characters · contract_max` | minimum=1; reason="schema-maximum" |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-764b8aa7ff"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-ea2a77f760"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDescription`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
