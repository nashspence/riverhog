# riverhog_protocol.PortableCollectionInventoryAuthority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-portablecollectioninven-cb5712bb2b:c7a58f0a5c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d63ed8a4d1"></a>
- <a id="s-0639af0917"></a>`distribution`: `riverhog-protocol`
- <a id="s-7722dce441"></a>`module`: `riverhog_protocol`
- <a id="s-b58fa2119e"></a>`name`: `PortableCollectionInventoryAuthority`
- <a id="s-1f229d4eb9"></a>`unit`: `export`

### Declared structure

- <a id="s-d31b14aa8a"></a>`kind`: `"class"`
- <a id="s-556cbcc9d9"></a>`signature`: `"\"(*, header: riverhog_protocol.portable_collection.PortableCollectionHeader, inventory_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], file_count: Annotated[int, Ge(ge=1)], file_bytes: Annotated[int, Ge(ge=0)]) -> None\""`

#### Validated model schema

<a id="s-3faf3f53b8"></a>

- <a id="s-6068f9ef62"></a>`type`: `"object"`
- <a id="s-89ebc20b7d"></a>`additionalProperties`: `false`
- <a id="s-e84db2a02b"></a>`required`: `["header","inventory_identity","file_count","file_bytes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5a27b4f2b8"></a>`file_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-40606344be"></a>`file_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-42cbdd55a0"></a>`header` | yes | [PortableCollectionHeader](#s-98fff72407) |  |
| <a id="s-925b6902ff"></a>`inventory_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [CollectionId](#s-d363bae8a7)
- [PortableCollectionHeader](#s-98fff72407)

##### <a id="s-d363bae8a7"></a>definition `CollectionId`

- <a id="s-02646d607d"></a>`type`: `"integer"`
- <a id="s-e0b4a1f722"></a>`minimum`: `1`

##### <a id="s-98fff72407"></a>definition `PortableCollectionHeader`

- <a id="s-6f79eed830"></a>`type`: `"object"`
- <a id="s-40f934aa5f"></a>`additionalProperties`: `false`
- <a id="s-8558561cbb"></a>`required`: `["collection","content_identity","encryption_format","passphrase_id","provenance_mode"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-40738de5dd"></a>`collection` | yes | [CollectionId](#s-d363bae8a7) |  |
| <a id="s-29f2935c59"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-995ef54dc2"></a>`encryption_format` | yes | type="string"; minLength=1 |  |
| <a id="s-c05f51718d"></a>`format` | no | type="string"; const="riverhog-collection/v1"; default="riverhog-collection/v1" |  |
| <a id="s-a8748b73bf"></a>`passphrase_id` | yes | type="string"; pattern="^[A-Za-z0-9_-]{16,128}$" |  |
| <a id="s-c6463915eb"></a>`provenance_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-f9273655ce"></a>`provenance_mode` | yes | type="string"; enum=["captured","mixed","omitted"] |  |

## Governing policies

- <a id="pa-895b4321e4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.PortableCollectionInventoryAuthority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 18da01b0153d422d85133d0802e30fb8732e0e1f2956d9de13b8fa9a273f6b18 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
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
        }
      },
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
    },
    "signature": "\"(*, header: riverhog_protocol.portable_collection.PortableCollectionHeader, inventory_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], file_count: Annotated[int, Ge(ge=1)], file_bytes: Annotated[int, Ge(ge=0)]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "PortableCollectionInventoryAuthority",
  "unit": "export"
}
```

</details>
