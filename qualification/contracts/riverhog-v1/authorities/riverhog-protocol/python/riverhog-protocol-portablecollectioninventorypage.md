# riverhog_protocol.PortableCollectionInventoryPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-portablecollectioninventorypage:180fca9afb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6a35447804"></a>
- <a id="s-802a817536"></a>`distribution`: `riverhog-protocol`
- <a id="s-4052838d48"></a>`module`: `riverhog_protocol`
- <a id="s-1e00a5d1ef"></a>`name`: `PortableCollectionInventoryPage`
- <a id="s-2d41295a95"></a>`unit`: `export`

### Declared structure

- <a id="s-bb5a8e4821"></a>`kind`: `"class"`
- <a id="s-32ad91debc"></a>`signature`: `"\"(*, format: Literal['riverhog-collection-inventory-page/v1'] = 'riverhog-collection-inventory-page/v1', authority: riverhog_protocol.portable_collection.PortableCollectionInventoryAuthority, files: Annotated[list[riverhog_protocol.file_identity.ImmutableFileIdentityDocument], MaxLen(max_length=1000)], next_cursor: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=8192)] = None, complete: bool) -> None\""`

#### Validated model schema

<a id="s-d7dbb6340d"></a>

- <a id="s-6f91f83dce"></a>`type`: `"object"`
- <a id="s-dc4b67e911"></a>`additionalProperties`: `false`
- <a id="s-fa312aacb3"></a>`required`: `["authority","files","complete"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5d0beadb56"></a>`authority` | yes | [PortableCollectionInventoryAuthority](#s-58d933375e) |  |
| <a id="s-f5a81c0a53"></a>`complete` | yes | type="boolean" |  |
| <a id="s-32163b904e"></a>`files` | yes | type="array"; items=([ImmutableFileIdentityDocument](#s-9185aadd43)); maxItems=1000; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"authority-bound-cursor","reason":"bounded-portable-inventory-page"} |  |
| <a id="s-daf906b4ae"></a>`format` | no | type="string"; const="riverhog-collection-inventory-page/v1"; default="riverhog-collection-inventory-page/v1" |  |
| <a id="s-a8c96c4171"></a>`next_cursor` | no | anyOf=[(type="string"; maxLength=8192; minLength=1); (type="null")]; default=null |  |

##### Definitions

- [CanonicalRelPath](#s-859b45e2c0)
- [CollectionId](#s-75898504ca)
- [ImmutableFileIdentityDocument](#s-9185aadd43)
- [PortableCollectionHeader](#s-c2615d6cd5)
- [PortableCollectionInventoryAuthority](#s-58d933375e)

##### <a id="s-859b45e2c0"></a>definition `CanonicalRelPath`

- <a id="s-15f9db64d9"></a>`type`: `"string"`
- <a id="s-708ec30e25"></a>`format`: `"riverhog-canonical-relpath-v1"`
- <a id="s-0a9473d302"></a>`maxLength`: `4096`
- <a id="s-04c1b33859"></a>`minLength`: `1`
- <a id="s-8e5f461b81"></a>`pattern`: `"^[^/\\\\]+(?:/[^/\\\\]+)*$"`
- <a id="s-988e470b99"></a>`x-unicode-normalization`: `"NFC"`

###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-d7d2a868e2"></a>1 | not=(pattern="(?:^\|/)\\.{1,2}(?:/\|$)") |
| <a id="s-1d4d69d54f"></a>2 | not=(pattern="^\\s\|\\s$") |

##### <a id="s-75898504ca"></a>definition `CollectionId`

- <a id="s-58b3433b8e"></a>`type`: `"integer"`
- <a id="s-826fa8b10e"></a>`minimum`: `1`

##### <a id="s-9185aadd43"></a>definition `ImmutableFileIdentityDocument`

- <a id="s-55524e5057"></a>`type`: `"object"`
- <a id="s-c4e4e3b9ac"></a>`additionalProperties`: `false`
- <a id="s-283f3e97de"></a>`required`: `["path","bytes","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2f0d8b60b0"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-335b811df3"></a>`path` | yes | [CanonicalRelPath](#s-859b45e2c0) |  |
| <a id="s-76d39360f5"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-c2615d6cd5"></a>definition `PortableCollectionHeader`

- <a id="s-416e25431d"></a>`type`: `"object"`
- <a id="s-73920817df"></a>`additionalProperties`: `false`
- <a id="s-9847e05d82"></a>`required`: `["collection","content_identity","encryption_format","passphrase_id","provenance_mode"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-268c1100e2"></a>`collection` | yes | [CollectionId](#s-75898504ca) |  |
| <a id="s-ca504cf17d"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6e123bf42d"></a>`encryption_format` | yes | type="string"; minLength=1 |  |
| <a id="s-1a6f7a11e9"></a>`format` | no | type="string"; const="riverhog-collection/v1"; default="riverhog-collection/v1" |  |
| <a id="s-0c90b603d9"></a>`passphrase_id` | yes | type="string"; pattern="^[A-Za-z0-9_-]{16,128}$" |  |
| <a id="s-c1e75c8c23"></a>`provenance_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-1e87596e88"></a>`provenance_mode` | yes | type="string"; enum=["captured","mixed","omitted"] |  |

##### <a id="s-58d933375e"></a>definition `PortableCollectionInventoryAuthority`

- <a id="s-db6b6ca068"></a>`type`: `"object"`
- <a id="s-d0d258419e"></a>`additionalProperties`: `false`
- <a id="s-f30203ebab"></a>`required`: `["header","inventory_identity","file_count","file_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b53a6f0d2e"></a>`file_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-a1d85e04cc"></a>`file_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-8b7a816e50"></a>`header` | yes | [PortableCollectionHeader](#s-c2615d6cd5) |  |
| <a id="s-d75447da31"></a>`inventory_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [validate_page](riverhog-protocol-portablecollectioninventorypage-validate-page.md)

## Governing policies

- <a id="pa-3e89fa838e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.PortableCollectionInventoryPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2cfad62b1e57a4b3781eb4338941550f960a122d23a4dcceb2a2e5ee180ba2c3 -->

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
        "ImmutableFileIdentityDocument": {
          "additionalProperties": false,
          "properties": {
            "bytes": {
              "minimum": 0,
              "type": "integer"
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
            "path",
            "bytes",
            "sha256"
          ],
          "type": "object"
        },
        "PortableCollectionHeader": {
          "additionalProperties": false,
          "properties": {
            "collection": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "encryption_format": {
              "minLength": 1,
              "type": "string"
            },
            "format": {
              "const": "riverhog-collection/v1",
              "default": "riverhog-collection/v1",
              "type": "string"
            },
            "passphrase_id": {
              "pattern": "^[A-Za-z0-9_-]{16,128}$",
              "type": "string"
            },
            "provenance_identity": {
              "anyOf": [
                {
                  "pattern": "^[0-9a-f]{64}$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "provenance_mode": {
              "enum": [
                "captured",
                "mixed",
                "omitted"
              ],
              "type": "string"
            }
          },
          "required": [
            "collection",
            "content_identity",
            "encryption_format",
            "passphrase_id",
            "provenance_mode"
          ],
          "type": "object"
        },
        "PortableCollectionInventoryAuthority": {
          "additionalProperties": false,
          "properties": {
            "file_bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "file_count": {
              "minimum": 1,
              "type": "integer"
            },
            "header": {
              "$ref": "#/$defs/PortableCollectionHeader"
            },
            "inventory_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "header",
            "inventory_identity",
            "file_count",
            "file_bytes"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "authority": {
          "$ref": "#/$defs/PortableCollectionInventoryAuthority"
        },
        "complete": {
          "type": "boolean"
        },
        "files": {
          "items": {
            "$ref": "#/$defs/ImmutableFileIdentityDocument"
          },
          "maxItems": 1000,
          "type": "array",
          "x-riverhog-extent": {
            "policy": "segmented_no_total_max",
            "progression": "authority-bound-cursor",
            "reason": "bounded-portable-inventory-page"
          }
        },
        "format": {
          "const": "riverhog-collection-inventory-page/v1",
          "default": "riverhog-collection-inventory-page/v1",
          "type": "string"
        },
        "next_cursor": {
          "anyOf": [
            {
              "maxLength": 8192,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "required": [
        "authority",
        "files",
        "complete"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['riverhog-collection-inventory-page/v1'] = 'riverhog-collection-inventory-page/v1', authority: riverhog_protocol.portable_collection.PortableCollectionInventoryAuthority, files: Annotated[list[riverhog_protocol.file_identity.ImmutableFileIdentityDocument], MaxLen(max_length=1000)], next_cursor: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=8192)] = None, complete: bool) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "PortableCollectionInventoryPage",
  "unit": "export"
}
```

</details>
