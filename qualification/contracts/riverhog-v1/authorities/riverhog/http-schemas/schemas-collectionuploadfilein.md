# schemas: CollectionUploadFileIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadfilein:c3cad12cb0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-db87f36645"></a>

- <a id="s-863177a48e"></a>`type`: `"object"`
- <a id="s-1d51d73d73"></a>`additionalProperties`: `false`
- <a id="s-083b34aa9f"></a>`required`: `["path","bytes","sha256"]`
- <a id="s-1c88ab5f2c"></a>`title`: `"CollectionUploadFileIn"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-489bd23c4b"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-f0fd54920a"></a>`path` | yes | [CanonicalRelPath](schemas-canonicalrelpath.md) |  |
| <a id="s-8ebdaac874"></a>`provenance` | no | anyOf=[(discriminator={"mapping":{"captured":"#/components/schemas/CapturedFileProvenanceBinding","omitted":"#/components/schemas/OmittedFileProvenanceBinding"},"propertyName":"status"}; oneOf=[([CapturedFileProvenanceBinding](schemas-capturedfileprovenancebinding.md)); ([OmittedFileProvenanceBinding](schemas-omittedfileprovenancebinding.md))]); (type="null")]; title="Provenance" |  |
| <a id="s-6d8d82cd49"></a>`raw_parts` | no | anyOf=[([CollectionUploadRawPartsIn](schemas-collectionuploadrawpartsin.md)); (type="null")] |  |
| <a id="s-b6ef046198"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-489bd23c4b) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-b6ef046198) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CanonicalRelPath](schemas-canonicalrelpath.md)
- [CapturedFileProvenanceBinding](schemas-capturedfileprovenancebinding.md)
- [CollectionUploadRawPartsIn](schemas-collectionuploadrawpartsin.md)
- [OmittedFileProvenanceBinding](schemas-omittedfileprovenancebinding.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c7dc0e534b"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-bd1ff3179d"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-436c79fd9e"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadFileIn`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1e7d4e892dc37bf368acdc769e28c09f91487ce087943be592b05aabcd3e6a16 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
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
    "raw_parts": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionUploadRawPartsIn"
        },
        {
          "type": "null"
        }
      ]
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
  "title": "CollectionUploadFileIn",
  "type": "object"
}
```

</details>
