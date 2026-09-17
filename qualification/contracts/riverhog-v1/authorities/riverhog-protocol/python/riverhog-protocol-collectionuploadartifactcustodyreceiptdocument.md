# riverhog_protocol.CollectionUploadArtifactCustodyReceiptDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadartifac-1c46d59ee4:1fc5e1c7cf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-088a252182"></a>
- <a id="s-fe7d236555"></a>`distribution`: `riverhog-protocol`
- <a id="s-f190b93197"></a>`module`: `riverhog_protocol`
- <a id="s-e1b1f2d405"></a>`name`: `CollectionUploadArtifactCustodyReceiptDocument`
- <a id="s-be3dd24fac"></a>`unit`: `export`

### Declared structure

- <a id="s-dc166b4040"></a>`kind`: `"class"`
- <a id="s-79c6d52e85"></a>`signature`: `"\"(*, format: Literal['riverhog-artifact-custody-receipt/v1'] = 'riverhog-artifact-custody-receipt/v1', collection_id: CollectionId, path: str, bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], archive_object_count: Annotated[int, Strict(strict=True), Ge(ge=1)], archive_object_set_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], receipt_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-e4009535b1"></a>

- <a id="s-5af6e0eda3"></a>`type`: `"object"`
- <a id="s-83431d8412"></a>`additionalProperties`: `false`
- <a id="s-48f0e0b67c"></a>`required`: `["collection_id","path","bytes","sha256","archive_object_count","archive_object_set_sha256","receipt_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5f24861dbc"></a>`archive_object_count` | yes | type="integer"; minimum=1 |  |
| <a id="s-4697eb4666"></a>`archive_object_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-098d868844"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-5d89499379"></a>`collection_id` | yes | [CollectionId](#s-08f619d576) |  |
| <a id="s-cb6df9e360"></a>`format` | no | type="string"; const="riverhog-artifact-custody-receipt/v1"; default="riverhog-artifact-custody-receipt/v1" |  |
| <a id="s-62de67a206"></a>`path` | yes | type="string" |  |
| <a id="s-dcf7b7c59d"></a>`receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fdea4a93de"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [CollectionId](#s-08f619d576)

##### <a id="s-08f619d576"></a>definition `CollectionId`

- <a id="s-62202c7345"></a>`type`: `"integer"`
- <a id="s-96c316f3c6"></a>`minimum`: `1`

## Maintained corroboration

### Related interface records

- [seal](riverhog-protocol-collectionuploadartifactcustodyreceiptdocument-seal.md)
- [validate_receipt](riverhog-protocol-collectionuploadartifactcustodyreceiptdocument-validate-receipt.md)
- [canonical_path](riverhog-protocol-collectionuploadartifactcustodyreceiptdocument-canonical-path.md)
- [canonical_collection_id](riverhog-protocol-collectionuploadartifactcustodyreceiptdocument-canonical-collection-id.md)

## Governing policies

- <a id="pa-503617e5f3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadArtifactCustodyReceiptDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b6ed5bbc78387e6b186ea77306873dad4b81831bc3d5455eb088ca2cc18b45a -->

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
        "archive_object_count": {
          "minimum": 1,
          "type": "integer"
        },
        "archive_object_set_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "collection_id": {
          "$ref": "#/$defs/CollectionId"
        },
        "format": {
          "const": "riverhog-artifact-custody-receipt/v1",
          "default": "riverhog-artifact-custody-receipt/v1",
          "type": "string"
        },
        "path": {
          "type": "string"
        },
        "receipt_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "collection_id",
        "path",
        "bytes",
        "sha256",
        "archive_object_count",
        "archive_object_set_sha256",
        "receipt_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['riverhog-artifact-custody-receipt/v1'] = 'riverhog-artifact-custody-receipt/v1', collection_id: CollectionId, path: str, bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], archive_object_count: Annotated[int, Strict(strict=True), Ge(ge=1)], archive_object_set_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], receipt_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadArtifactCustodyReceiptDocument",
  "unit": "export"
}
```

</details>
