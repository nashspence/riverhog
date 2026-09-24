# schemas: ListCollectionFileProvenanceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-listcollectionfileprovenanceout:c53aa37ebf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-fd798adc6b"></a>

- <a id="s-9f911aa0e3"></a>`discriminator`: `{"mapping":{"captured":"#/components/schemas/CapturedCollectionFileProvenancePage","mixed":"#/components/schemas/MixedCollectionFileProvenancePage","omitted":"#/components/schemas/OmittedCollectionFileProvenancePage"},"propertyName":"provenance_mode"}`
- <a id="s-323f8c0d91"></a>`title`: `"ListCollectionFileProvenanceOut"`

### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| <a id="s-fe5eed434a"></a>1 | [CapturedCollectionFileProvenancePage](schemas-capturedcollectionfileprovenancepage.md) |
| <a id="s-0c42532448"></a>2 | [MixedCollectionFileProvenancePage](schemas-mixedcollectionfileprovenancepage.md) |
| <a id="s-09d0091400"></a>3 | [OmittedCollectionFileProvenancePage](schemas-omittedcollectionfileprovenancepage.md) |

## Maintained corroboration

### Referenced contract elements

- [CapturedCollectionFileProvenancePage](schemas-capturedcollectionfileprovenancepage.md)
- [MixedCollectionFileProvenancePage](schemas-mixedcollectionfileprovenancepage.md)
- [OmittedCollectionFileProvenancePage](schemas-omittedcollectionfileprovenancepage.md)

## Governing policies

- <a id="pa-9c67808663"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ListCollectionFileProvenanceOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fa592eae00db261df0f7dbd3ecdd70930b287ab34811552cc24dd7061e3fa3ea -->

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
  "title": "ListCollectionFileProvenanceOut"
}
```

</details>
