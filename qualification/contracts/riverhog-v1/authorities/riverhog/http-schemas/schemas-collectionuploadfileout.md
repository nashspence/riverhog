# schemas: CollectionUploadFileOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadfileout:984d0436b0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-e4f8cab7c7"></a>

- <a id="s-ef2c474e9d"></a>`type`: `"object"`
- <a id="s-7a625e070b"></a>`additionalProperties`: `false`
- <a id="s-a92662eea1"></a>`required`: `["path","bytes","sha256"]`
- <a id="s-d38617d68c"></a>`title`: `"CollectionUploadFileOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2eda695e01"></a>`bytes` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md) |  |
| <a id="s-57decd2dfa"></a>`custody_receipt` | no | anyOf=[([CollectionUploadArtifactCustodyReceiptDocument](schemas-collectionuploadartifactcustodyreceiptdocument.md)); (type="null")] |  |
| <a id="s-e476e99a4f"></a>`path` | yes | [CanonicalRelPath](schemas-canonicalrelpath.md) |  |
| <a id="s-954031ac14"></a>`provenance` | no | anyOf=[(discriminator={"mapping":{"captured":"#/components/schemas/CapturedFileProvenanceBinding","omitted":"#/components/schemas/OmittedFileProvenanceBinding"},"propertyName":"status"}; oneOf=[([CapturedFileProvenanceBinding](schemas-capturedfileprovenancebinding.md)); ([OmittedFileProvenanceBinding](schemas-omittedfileprovenancebinding.md))]); (type="null")]; title="Provenance" |  |
| <a id="s-e71d2c6050"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-e71d2c6050) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CanonicalRelPath](schemas-canonicalrelpath.md)
- [CapturedFileProvenanceBinding](schemas-capturedfileprovenancebinding.md)
- [CollectionUploadArtifactCustodyReceiptDocument](schemas-collectionuploadartifactcustodyreceiptdocument.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)
- [OmittedFileProvenanceBinding](schemas-omittedfileprovenancebinding.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-04fed07023"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-fbf0cf30ce"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadFileOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ab1f75468ec6d444c70b3ba04d5c400ef63ff471c634056d1dd1ec17ddeee449 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "bytes": {
      "$ref": "#/components/schemas/NonnegativeDecimal"
    },
    "custody_receipt": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionUploadArtifactCustodyReceiptDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "path": {
      "$ref": "#/components/schemas/CanonicalRelPath"
    },
    "provenance": {
      "anyOf": [
        {
          "discriminator": {
            "mapping": {
              "captured": "#/components/schemas/CapturedFileProvenanceBinding",
              "omitted": "#/components/schemas/OmittedFileProvenanceBinding"
            },
            "propertyName": "status"
          },
          "oneOf": [
            {
              "$ref": "#/components/schemas/CapturedFileProvenanceBinding"
            },
            {
              "$ref": "#/components/schemas/OmittedFileProvenanceBinding"
            }
          ]
        },
        {
          "type": "null"
        }
      ],
      "title": "Provenance"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "path",
    "bytes",
    "sha256"
  ],
  "title": "CollectionUploadFileOut",
  "type": "object"
}
```

</details>
