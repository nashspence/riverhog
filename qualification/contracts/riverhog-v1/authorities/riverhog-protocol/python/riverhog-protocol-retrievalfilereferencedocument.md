# riverhog_protocol.RetrievalFileReferenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retrievalfilereferencedocument:eb093ca910 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7002b79901"></a>
- <a id="s-444f550f0a"></a>`distribution`: `riverhog-protocol`
- <a id="s-2f3c197753"></a>`module`: `riverhog_protocol`
- <a id="s-612384c044"></a>`name`: `RetrievalFileReferenceDocument`
- <a id="s-415503c74c"></a>`unit`: `export`

### Declared structure

- <a id="s-80f3fc63c2"></a>`kind`: `"class"`
- <a id="s-e148c307bf"></a>`signature`: `"'(*, collection_id: CollectionId, path: CanonicalRelPath) -> None'"`

#### Validated model schema

<a id="s-ae554c5d48"></a>

- <a id="s-b897be87c8"></a>`type`: `"object"`
- <a id="s-6b289f9429"></a>`additionalProperties`: `false`
- <a id="s-e25c97a6e6"></a>`required`: `["collection_id","path"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7be108cddd"></a>`collection_id` | yes | [CollectionId](#s-66b4e9bd4e) |  |
| <a id="s-7e9ef988be"></a>`path` | yes | [CanonicalRelPath](#s-63a8c2807f) |  |

##### Definitions

- [CanonicalRelPath](#s-63a8c2807f)
- [CollectionId](#s-66b4e9bd4e)

##### <a id="s-63a8c2807f"></a>definition `CanonicalRelPath`

- <a id="s-6cf8697779"></a>`type`: `"string"`
- <a id="s-1f955ea12d"></a>`format`: `"riverhog-canonical-relpath-v1"`
- <a id="s-4caa214984"></a>`maxLength`: `4096`
- <a id="s-4841c3f7b8"></a>`minLength`: `1`
- <a id="s-ef29d2abed"></a>`pattern`: `"^[^/\\\\]+(?:/[^/\\\\]+)*$"`
- <a id="s-b415c93e91"></a>`x-unicode-normalization`: `"NFC"`

###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-873506c0b9"></a>1 | not=(pattern="(?:^\|/)\\.{1,2}(?:/\|$)") |
| <a id="s-acd960eaf2"></a>2 | not=(pattern="^\\s\|\\s$") |

##### <a id="s-66b4e9bd4e"></a>definition `CollectionId`

- <a id="s-333d924024"></a>`type`: `"integer"`
- <a id="s-eaa21623bc"></a>`minimum`: `1`

## Governing policies

- <a id="pa-f8c9fe0c85"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.RetrievalFileReferenceDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f43d9352a1b1c98a32b53183e13020fecb8d4f508ac6fd61ff83ba78b280377 -->

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
        }
      },
      "additionalProperties": false,
      "properties": {
        "collection_id": {
          "$ref": "#/$defs/CollectionId"
        },
        "path": {
          "$ref": "#/$defs/CanonicalRelPath"
        }
      },
      "required": [
        "collection_id",
        "path"
      ],
      "type": "object"
    },
    "signature": "'(*, collection_id: CollectionId, path: CanonicalRelPath) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RetrievalFileReferenceDocument",
  "unit": "export"
}
```

</details>
