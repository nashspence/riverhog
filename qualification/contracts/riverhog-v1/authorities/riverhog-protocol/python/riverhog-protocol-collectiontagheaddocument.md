# riverhog_protocol.CollectionTagHeadDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagheaddocument:2f399c40f1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d87acca02f"></a>
- <a id="s-2f08781b6f"></a>`distribution`: `riverhog-protocol`
- <a id="s-34e21bc7ea"></a>`module`: `riverhog_protocol`
- <a id="s-54b5e7f799"></a>`name`: `CollectionTagHeadDocument`
- <a id="s-2318c138cc"></a>`unit`: `export`

### Declared structure

- <a id="s-33070c9f88"></a>`kind`: `"class"`
- <a id="s-9a2ec9f60b"></a>`signature`: `"\"(*, format: Literal['riverhog-collection-tag-head/v1'] = 'riverhog-collection-tag-head/v1', archive_root_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], revision: Annotated[int, Strict(strict=True), Ge(ge=1), Le(le=9007199254740991)], root_sha256: Annotated[str \| None, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')] = None, tag_set_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], head_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-ce4ab1a01c"></a>
- <a id="s-6ca05613b3"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a1c9e422b3"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6d8f707f07"></a>`format` | no | type="string"; const="riverhog-collection-tag-head/v1" |  |
| <a id="s-ce95c34a50"></a>`head_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-c714f6110c"></a>`revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-1381339e01"></a>`root_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-1f359cc827"></a>`tag_set_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionTagHeadDocument.validate_identities](riverhog-protocol-collectiontagheaddocument-validate-identities.md)
- [riverhog_protocol.CollectionTagHeadDocument.to_json_bytes](riverhog-protocol-collectiontagheaddocument-to-json-bytes.md)
- [riverhog_protocol.CollectionTagHeadDocument.from_json_bytes](riverhog-protocol-collectiontagheaddocument-from-json-bytes.md)
- [riverhog_protocol.CollectionTagHeadDocument.seal](riverhog-protocol-collectiontagheaddocument-seal.md)

## Governing policies

- <a id="pa-5061c62fe1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagHeadDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f2cc77ada412a915f7cfae31cf738dfdc026c5423bc466ef78878b5e899d3227 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "archive_root_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "format": {
          "const": "riverhog-collection-tag-head/v1",
          "default": "riverhog-collection-tag-head/v1",
          "type": "string"
        },
        "head_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "revision": {
          "maximum": 9007199254740991,
          "minimum": 1,
          "type": "integer"
        },
        "root_sha256": {
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
        "tag_set_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "archive_root_sha256",
        "revision",
        "tag_set_identity",
        "head_identity"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['riverhog-collection-tag-head/v1'] = 'riverhog-collection-tag-head/v1', archive_root_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], revision: Annotated[int, Strict(strict=True), Ge(ge=1), Le(le=9007199254740991)], root_sha256: Annotated[str | None, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')] = None, tag_set_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], head_identity: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionTagHeadDocument",
  "unit": "export"
}
```
