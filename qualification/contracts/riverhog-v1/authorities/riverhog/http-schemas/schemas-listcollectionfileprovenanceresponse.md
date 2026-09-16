# schemas: ListCollectionFileProvenanceResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-listcollectionfileprovenanceresponse:65bb4b951c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-62bcebadbd"></a>

- <a id="s-4713dc9f60"></a>`discriminator`: `{"mapping":{"captured":"#/components/schemas/CapturedCollectionFileProvenancePage","mixed":"#/components/schemas/MixedCollectionFileProvenancePage","omitted":"#/components/schemas/OmittedCollectionFileProvenancePage"},"propertyName":"provenance_mode"}`
- <a id="s-bf7392250b"></a>`title`: `"ListCollectionFileProvenanceResponse"`

### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| <a id="s-aade95a74c"></a>1 | [CapturedCollectionFileProvenancePage](schemas-capturedcollectionfileprovenancepage.md) |
| <a id="s-cf584773f9"></a>2 | [MixedCollectionFileProvenancePage](schemas-mixedcollectionfileprovenancepage.md) |
| <a id="s-042f4445f6"></a>3 | [OmittedCollectionFileProvenancePage](schemas-omittedcollectionfileprovenancepage.md) |

## Maintained corroboration

### Referenced contract dossiers

- [CapturedCollectionFileProvenancePage](schemas-capturedcollectionfileprovenancepage.md)
- [MixedCollectionFileProvenancePage](schemas-mixedcollectionfileprovenancepage.md)
- [OmittedCollectionFileProvenancePage](schemas-omittedcollectionfileprovenancepage.md)

## Governing policies

- <a id="pa-6ed3b1d586"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ListCollectionFileProvenanceResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1958a6ac97dfb941a4083e304b6d96461650baf008d19010a45960c7aa31ed38 -->

```json
{
  "discriminator": {
    "mapping": {
      "captured": "#/components/schemas/CapturedCollectionFileProvenancePage",
      "mixed": "#/components/schemas/MixedCollectionFileProvenancePage",
      "omitted": "#/components/schemas/OmittedCollectionFileProvenancePage"
    },
    "propertyName": "provenance_mode"
  },
  "oneOf": [
    {
      "$ref": "#/components/schemas/CapturedCollectionFileProvenancePage"
    },
    {
      "$ref": "#/components/schemas/MixedCollectionFileProvenancePage"
    },
    {
      "$ref": "#/components/schemas/OmittedCollectionFileProvenancePage"
    }
  ],
  "title": "ListCollectionFileProvenanceResponse"
}
```

</details>
