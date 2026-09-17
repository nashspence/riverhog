# riverhog_protocol.CollectionUploadFileIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadfilein:f0086a248d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d65abb63f3"></a>
- <a id="s-d05122f27b"></a>`distribution`: `riverhog-protocol`
- <a id="s-267f254c58"></a>`module`: `riverhog_protocol`
- <a id="s-cc2efd5537"></a>`name`: `CollectionUploadFileIn`
- <a id="s-0c59057afa"></a>`unit`: `export`

### Declared structure

- <a id="s-f7ee9ce2f4"></a>`kind`: `"class"`
- <a id="s-650f7668c9"></a>`signature`: `"\"(*, path: CanonicalRelPath, bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], raw_parts: riverhog_protocol.collection_upload_transport.CollectionUploadRawPartsIn \| None = None, provenance: Optional[Annotated[riverhog_protocol.collection_upload_transport.CapturedFileProvenanceBinding \| riverhog_protocol.collection_upload_transport.OmittedFileProvenanceBinding, FieldInfo(annotation=NoneType, required=True, discriminator='status')]] = None) -> None\""`

#### Validated model schema

<a id="s-ac3087e8e5"></a>

- <a id="s-431c1cf8c3"></a>`type`: `"object"`
- <a id="s-9518e0664a"></a>`additionalProperties`: `false`
- <a id="s-07467b6129"></a>`required`: `["path","bytes","sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8ef0950b9b"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-7e7965894e"></a>`path` | yes | [CanonicalRelPath](#s-2c127c38b4) |  |
| <a id="s-f6526b2622"></a>`provenance` | no | anyOf=[(discriminator={"mapping":{"captured":"#/$defs/CapturedFileProvenanceBinding","omitted":"#/$defs/OmittedFileProvenanceBinding"},"propertyName":"status"}; oneOf=[([CapturedFileProvenanceBinding](#s-e4b2dfa1c2)); ([OmittedFileProvenanceBinding](#s-774afcbe22))]); (type="null")]; default=null |  |
| <a id="s-d708f72f7f"></a>`raw_parts` | no | anyOf=[([CollectionUploadRawPartsIn](#s-9617869a96)); (type="null")]; default=null |  |
| <a id="s-d662903a5f"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [CanonicalRelPath](#s-2c127c38b4)
- [CapturedFileProvenanceBinding](#s-e4b2dfa1c2)
- [CollectionUploadRawPartsIn](#s-9617869a96)
- [OmittedFileProvenanceBinding](#s-774afcbe22)
- [ProvenanceJournalId](#s-71d9f30b54)
- [ProvenanceStateId](#s-31a410e8a3)

##### <a id="s-2c127c38b4"></a>definition `CanonicalRelPath`

- <a id="s-b9496795ac"></a>`type`: `"string"`
- <a id="s-6735846618"></a>`format`: `"riverhog-canonical-relpath-v1"`
- <a id="s-d27e2c3720"></a>`maxLength`: `4096`
- <a id="s-caeb44ae68"></a>`minLength`: `1`
- <a id="s-20edd778a4"></a>`pattern`: `"^[^/\\\\]+(?:/[^/\\\\]+)*$"`
- <a id="s-22239b79cf"></a>`x-unicode-normalization`: `"NFC"`

###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-230558973c"></a>1 | not=(pattern="(?:^\|/)\\.{1,2}(?:/\|$)") |
| <a id="s-a41f196552"></a>2 | not=(pattern="^\\s\|\\s$") |

##### <a id="s-e4b2dfa1c2"></a>definition `CapturedFileProvenanceBinding`

- <a id="s-6184453edd"></a>`type`: `"object"`
- <a id="s-c77aa1f299"></a>`additionalProperties`: `false`
- <a id="s-15d2473e5b"></a>`required`: `["journal_id","current_state_id","status"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-109bf645de"></a>`current_state_id` | yes | [ProvenanceStateId](#s-31a410e8a3) |  |
| <a id="s-f1800ef574"></a>`journal_id` | yes | [ProvenanceJournalId](#s-71d9f30b54) |  |
| <a id="s-72e276dc82"></a>`status` | yes | type="string"; const="captured" |  |

##### <a id="s-9617869a96"></a>definition `CollectionUploadRawPartsIn`

- <a id="s-c0f3611bc9"></a>`type`: `"object"`
- <a id="s-0b30457318"></a>`additionalProperties`: `false`
- <a id="s-50c453cae5"></a>`required`: `["part_plaintext_bytes","part_count","ordered_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9caff934d7"></a>`ordered_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-1c9e32d236"></a>`part_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-74bf523b87"></a>`part_plaintext_bytes` | yes | type="integer"; minimum=65536 |  |

##### <a id="s-774afcbe22"></a>definition `OmittedFileProvenanceBinding`

- <a id="s-a96d69a283"></a>`type`: `"object"`
- <a id="s-1ba932c334"></a>`additionalProperties`: `false`
- <a id="s-6b18de5f37"></a>`required`: `["status","omission_reason"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-27a4e7ac40"></a>`omission_reason` | yes | type="string"; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$" |  |
| <a id="s-5a990e3352"></a>`status` | yes | type="string"; const="omitted" |  |

##### <a id="s-71d9f30b54"></a>definition `ProvenanceJournalId`

- <a id="s-78dd0592a4"></a>`type`: `"string"`
- <a id="s-388d30ce72"></a>`pattern`: `"^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"`

##### <a id="s-31a410e8a3"></a>definition `ProvenanceStateId`

- <a id="s-2b9a1dfe2a"></a>`type`: `"string"`
- <a id="s-09131f3abf"></a>`pattern`: `"^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"`

## Governing policies

- <a id="pa-2e4809c9bd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadFileIn`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3664ca7a6914a0b33af38ab7513047cd77d2920878a23e08fff5544fa8252679 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CanonicalRelPath": {
          "allOf": [
            {
              "not": {
                "pattern": "(?:^|/)\\.{1,2}(?:/|$)"
              }
            },
            {
              "not": {
                "pattern": "^\\s|\\s$"
              }
            }
          ],
          "format": "riverhog-canonical-relpath-v1",
          "maxLength": 4096,
          "minLength": 1,
          "pattern": "^[^/\\\\]+(?:/[^/\\\\]+)*$",
          "type": "string",
          "x-unicode-normalization": "NFC"
        },
        "CapturedFileProvenanceBinding": {
          "additionalProperties": false,
          "properties": {
            "current_state_id": {
              "$ref": "#/$defs/ProvenanceStateId"
            },
            "journal_id": {
              "$ref": "#/$defs/ProvenanceJournalId"
            },
            "status": {
              "const": "captured",
              "type": "string"
            }
          },
          "required": [
            "journal_id",
            "current_state_id",
            "status"
          ],
          "type": "object"
        },
        "CollectionUploadRawPartsIn": {
          "additionalProperties": false,
          "properties": {
            "ordered_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "part_count": {
              "minimum": 1,
              "type": "integer"
            },
            "part_plaintext_bytes": {
              "minimum": 65536,
              "type": "integer"
            }
          },
          "required": [
            "part_plaintext_bytes",
            "part_count",
            "ordered_sha256"
          ],
          "type": "object"
        },
        "OmittedFileProvenanceBinding": {
          "additionalProperties": false,
          "properties": {
            "omission_reason": {
              "minLength": 1,
              "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
              "type": "string"
            },
            "status": {
              "const": "omitted",
              "type": "string"
            }
          },
          "required": [
            "status",
            "omission_reason"
          ],
          "type": "object"
        },
        "ProvenanceJournalId": {
          "pattern": "^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
          "type": "string"
        },
        "ProvenanceStateId": {
          "pattern": "^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "path": {
          "$ref": "#/$defs/CanonicalRelPath"
        },
        "provenance": {
          "anyOf": [
            {
              "discriminator": {
                "mapping": {
                  "captured": "#/$defs/CapturedFileProvenanceBinding",
                  "omitted": "#/$defs/OmittedFileProvenanceBinding"
                },
                "propertyName": "status"
              },
              "oneOf": [
                {
                  "$ref": "#/$defs/CapturedFileProvenanceBinding"
                },
                {
                  "$ref": "#/$defs/OmittedFileProvenanceBinding"
                }
              ]
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "raw_parts": {
          "anyOf": [
            {
              "$ref": "#/$defs/CollectionUploadRawPartsIn"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "path",
        "bytes",
        "sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, path: CanonicalRelPath, bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], raw_parts: riverhog_protocol.collection_upload_transport.CollectionUploadRawPartsIn | None = None, provenance: Optional[Annotated[riverhog_protocol.collection_upload_transport.CapturedFileProvenanceBinding | riverhog_protocol.collection_upload_transport.OmittedFileProvenanceBinding, FieldInfo(annotation=NoneType, required=True, discriminator='status')]] = None) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadFileIn",
  "unit": "export"
}
```

</details>
