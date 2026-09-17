# stove0_protocol.CollectionRootRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-collectionrootref:fc7187931c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-10a2a32df8"></a>
- <a id="s-0246f8a2d7"></a>`distribution`: `stove0-protocol`
- <a id="s-9da9fbb81a"></a>`module`: `stove0_protocol`
- <a id="s-a7ea5e1aa7"></a>`name`: `CollectionRootRef`
- <a id="s-ca40bdeee3"></a>`unit`: `export`

### Declared structure

- <a id="s-cffc0acbf1"></a>`kind`: `"class"`
- <a id="s-524d43c3bc"></a>`signature`: `"\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], content_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-9e39aea4c3"></a>

- <a id="s-6a9c1d36d4"></a>`type`: `"object"`
- <a id="s-db3d5e6105"></a>`additionalProperties`: `false`
- <a id="s-ca426c691d"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-092c9b7dae"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5044a4d3bc"></a>`collection_id` | yes | [CollectionId](#s-a0dd024c6d) |  |
| <a id="s-bb3ed71587"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [CollectionId](#s-a0dd024c6d)

##### <a id="s-a0dd024c6d"></a>definition `CollectionId`

- <a id="s-fe67a67168"></a>`type`: `"integer"`
- <a id="s-af909101f4"></a>`minimum`: `1`

## Maintained corroboration

### Related interface records

- [from_identity](stove0-protocol-collectionrootref-from-identity.md)
- [to_identity](stove0-protocol-collectionrootref-to-identity.md)

## Governing policies

- <a id="pa-5e315efac0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.CollectionRootRef`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 103a61dcbfc6a28ec9ef3e4871d0ee2cac7f04b4abb250ccc0ea5c3f08fc8d42 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        }
      },
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
    },
    "signature": "\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], content_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "CollectionRootRef",
  "unit": "export"
}
```

</details>
