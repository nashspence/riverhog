# schemas: CollectionUploadFileIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadfilein:38d42c5c93 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-db87f36645"></a>
- <a id="s-1c88ab5f2c"></a>`title`: CollectionUploadFileIn
- <a id="s-863177a48e"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-489bd23c4b"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-f0fd54920a"></a>`path` | yes | #/components/schemas/CanonicalRelPath |  |
| <a id="s-8ebdaac874"></a>`provenance` | no | anyOf=oneOf=#/components/schemas/CapturedFileProvenanceBinding \| #/components/schemas/OmittedFileProvenanceBinding; additional keys=`discriminator` \| type="null" |  |
| <a id="s-6d8d82cd49"></a>`raw_parts` | no | anyOf=#/components/schemas/CollectionUploadRawPartsIn \| type="null" |  |
| <a id="s-b6ef046198"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-489bd23c4b) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-b6ef046198) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CanonicalRelPath](schemas-canonicalrelpath.md)
- [schemas: CapturedFileProvenanceBinding](schemas-capturedfileprovenancebinding.md)
- [schemas: CollectionUploadRawPartsIn](schemas-collectionuploadrawpartsin.md)
- [schemas: OmittedFileProvenanceBinding](schemas-omittedfileprovenancebinding.md)

## Governing policies

- <a id="pa-0c0ce062bd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-dbc3c363c8"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-cfffa98ff4"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadFileIn`

### Exact owned JSON

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
