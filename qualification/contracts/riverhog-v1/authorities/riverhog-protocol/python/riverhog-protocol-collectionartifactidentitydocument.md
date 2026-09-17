# riverhog_protocol.CollectionArtifactIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionartifactidentitydocument:035869f261 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-956255dfcd"></a>
- <a id="s-3d98dc5a07"></a>`distribution`: `riverhog-protocol`
- <a id="s-76bba8821f"></a>`module`: `riverhog_protocol`
- <a id="s-d8b8ba46c8"></a>`name`: `CollectionArtifactIdentityDocument`
- <a id="s-7b1aba30ae"></a>`unit`: `export`

### Declared structure

- <a id="s-795f655a4a"></a>`kind`: `"class"`
- <a id="s-934645b5d4"></a>`signature`: `"\"(*, collection: riverhog_protocol.collection_workflow_transport.CollectionRootIdentityDocument, path: CanonicalRelPath, bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-ab068bfb41"></a>

- <a id="s-8bc12bb18d"></a>`type`: `"object"`
- <a id="s-39558216d7"></a>`additionalProperties`: `false`
- <a id="s-7424343afe"></a>`required`: `["collection","path","bytes","sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-57e3627403"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-8bc6c9ef5a"></a>`collection` | yes | [CollectionRootIdentityDocument](#s-6b0a0a299b) |  |
| <a id="s-1e37004564"></a>`path` | yes | [CanonicalRelPath](#s-36e7ee4bca) |  |
| <a id="s-bba78bc748"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [CanonicalRelPath](#s-36e7ee4bca)
- [CollectionId](#s-1dc1d018d5)
- [CollectionRootIdentityDocument](#s-6b0a0a299b)

##### <a id="s-36e7ee4bca"></a>definition `CanonicalRelPath`

- <a id="s-65272da6fb"></a>`type`: `"string"`
- <a id="s-78ef0d9bda"></a>`format`: `"riverhog-canonical-relpath-v1"`
- <a id="s-0a7896c8fd"></a>`maxLength`: `4096`
- <a id="s-e6577a27f8"></a>`minLength`: `1`
- <a id="s-ef2d5f79e4"></a>`pattern`: `"^[^/\\\\]+(?:/[^/\\\\]+)*$"`
- <a id="s-33caa0d8ee"></a>`x-unicode-normalization`: `"NFC"`

###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-527c13a876"></a>1 | not=(pattern="(?:^\|/)\\.{1,2}(?:/\|$)") |
| <a id="s-260f497939"></a>2 | not=(pattern="^\\s\|\\s$") |

##### <a id="s-1dc1d018d5"></a>definition `CollectionId`

- <a id="s-4437a99e67"></a>`type`: `"integer"`
- <a id="s-c5dc484541"></a>`minimum`: `1`

##### <a id="s-6b0a0a299b"></a>definition `CollectionRootIdentityDocument`

- <a id="s-974a81b844"></a>`type`: `"object"`
- <a id="s-00dee2deb3"></a>`additionalProperties`: `false`
- <a id="s-f8a494991a"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-939988ac85"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-05fb14f443"></a>`collection_id` | yes | [CollectionId](#s-1dc1d018d5) |  |
| <a id="s-29a68e9f99"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-collectionartifactidentitydocument-getitem.md)
- [validate_identity](riverhog-protocol-collectionartifactidentitydocument-validate-identity.md)
- [get](riverhog-protocol-collectionartifactidentitydocument-get.md)

## Governing policies

- <a id="pa-faff1b87a8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionArtifactIdentityDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0e93476172ab2132154f6795f00f535f628c7c46f8b12251a739c86dabde5a27 -->

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
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        },
        "CollectionRootIdentityDocument": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "collection": {
          "$ref": "#/$defs/CollectionRootIdentityDocument"
        },
        "path": {
          "$ref": "#/$defs/CanonicalRelPath"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "collection",
        "path",
        "bytes",
        "sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, collection: riverhog_protocol.collection_workflow_transport.CollectionRootIdentityDocument, path: CanonicalRelPath, bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionArtifactIdentityDocument",
  "unit": "export"
}
```

</details>
