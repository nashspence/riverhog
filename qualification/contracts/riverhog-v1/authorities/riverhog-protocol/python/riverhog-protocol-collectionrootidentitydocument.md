# riverhog_protocol.CollectionRootIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionrootidentitydocument:2894e47535 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b9c1e1d8ca"></a>
- <a id="s-a8298c29cb"></a>`distribution`: `riverhog-protocol`
- <a id="s-0ab7238084"></a>`module`: `riverhog_protocol`
- <a id="s-bb2a873c77"></a>`name`: `CollectionRootIdentityDocument`
- <a id="s-6bb54a8bdf"></a>`unit`: `export`

### Declared structure

- <a id="s-6948c7fb82"></a>`kind`: `"class"`
- <a id="s-5ac7c021a6"></a>`signature`: `"\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], content_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-a52bb9ed6b"></a>
- <a id="s-8bf61192c9"></a>`title`: CollectionRootIdentityDocument
- <a id="s-40e688420a"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d876a1dc7d"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e9209758b4"></a>`collection_id` | yes | #/$defs/CollectionId |  |
| <a id="s-f25b0ddace"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-7601e51a21"></a>`CollectionId` | type="integer"; minimum=1 |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionRootIdentityDocument.validate_identity](riverhog-protocol-collectionrootidentitydocument-validate-identity.md)

## Governing policies

- <a id="pa-e8fe5095eb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionRootIdentityDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8fe280f3a4ed3bfa2f8ccd8927b02431a44391b0a4fcbc812fe7dff63845e4bc -->

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
          "title": "Archive Root Sha256",
          "type": "string"
        },
        "collection_id": {
          "$ref": "#/$defs/CollectionId"
        },
        "content_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Content Identity",
          "type": "string"
        }
      },
      "required": [
        "collection_id",
        "archive_root_sha256",
        "content_identity"
      ],
      "title": "CollectionRootIdentityDocument",
      "type": "object"
    },
    "signature": "\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], content_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionRootIdentityDocument",
  "unit": "export"
}
```
