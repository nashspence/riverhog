# schemas: ProcessingClaimSettleDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-processingclaimsettledocument:53e0d4cf21 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-53b164176d"></a>

- <a id="s-7cefd7d377"></a>`type`: `"object"`
- <a id="s-aa276b43d8"></a>`additionalProperties`: `false`
- <a id="s-f0d3cce35c"></a>`required`: `["fence","output_collection_id","derivation"]`
- <a id="s-c084ae42f8"></a>`title`: `"ProcessingClaimSettleDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2c33191341"></a>`derivation` | yes | [CollectionDerivationDocument](schemas-collectionderivationdocument.md) |  |
| <a id="s-3353cd3ebf"></a>`fence` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-593c65a4f2"></a>`outcome` | no | anyOf=[([ProcessingOutcomeBindingDocument](schemas-processingoutcomebindingdocument.md)); (type="null")] |  |
| <a id="s-bc8bb48614"></a>`output_collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |

## Maintained corroboration

### Referenced contract elements

- [CollectionDerivationDocument](schemas-collectionderivationdocument.md)
- [CollectionId](schemas-collectionid.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)
- [ProcessingOutcomeBindingDocument](schemas-processingoutcomebindingdocument.md)

## Governing policies

- <a id="pa-43e353e2f6"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingClaimSettleDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b590d53ed3d41814f5fc69704c4d88fb534b9b8a8dcd3b19ea5a3ea1a7758ae8 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "derivation": {
      "$ref": "#/components/schemas/CollectionDerivationDocument"
    },
    "fence": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 1
    },
    "outcome": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ProcessingOutcomeBindingDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "output_collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    }
  },
  "required": [
    "fence",
    "output_collection_id",
    "derivation"
  ],
  "title": "ProcessingClaimSettleDocument",
  "type": "object"
}
```

</details>
