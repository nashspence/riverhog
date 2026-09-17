# schemas: CollectionDescription

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-collectiondescription:efda69f293 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-a9656ba901"></a>

- <a id="s-e92fe0fa37"></a>`type`: `"string"`
- <a id="s-c5789880fe"></a>`maxLength`: `32768`
- <a id="s-58fa788f9c"></a>`minLength`: `1`
- <a id="s-1961afea35"></a>`x-riverhog-encoded-bytes-max`: `32768`
- <a id="s-5dce35f005"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-catalog-description"}`
- <a id="s-6274b67d83"></a>`x-unicode-normalization`: `"NFC"`

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=32768

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [schemas: CollectionDescription](#s-a9656ba901) | `encoded-size · bytes · contract_max` | reason="bounded-human-authored-catalog-description"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [schemas: CollectionDescription](#s-a9656ba901) | `length · characters · contract_max` | minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-7c05028a65"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-cffbfa934f"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/CollectionDescription`

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
