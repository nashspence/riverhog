# riverhog_protocol.RetrievalFileReferenceSetDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retrievalfilereferencesetdocument:87cb8658a6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1115155719"></a>
- <a id="s-59bf0e8cee"></a>`distribution`: `riverhog-protocol`
- <a id="s-1c628d835b"></a>`module`: `riverhog_protocol`
- <a id="s-a2a199460a"></a>`name`: `RetrievalFileReferenceSetDocument`
- <a id="s-ce9fd8f62c"></a>`unit`: `export`

### Declared structure

- <a id="s-0c07729b51"></a>`kind`: `"class"`
- <a id="s-647b32cae2"></a>`signature`: `"'(*, files: Annotated[list[riverhog_protocol.retrieval_transport.RetrievalFileReferenceDocument], MinLen(min_length=1), MaxLen(max_length=10000)]) -> None'"`

#### Validated model schema

<a id="s-379ee065cb"></a>
- <a id="s-107276300f"></a>`title`: RetrievalFileReferenceSetDocument
- <a id="s-b3278f72da"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-968becece6"></a>`files` | yes | type="array"; minItems=1; maxItems=10000; items=(#/$defs/RetrievalFileReferenceDocument); additional keys=`x-riverhog-extent` |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-1828163c8e"></a>`CanonicalRelPath` | type="string"; format="riverhog-canonical-relpath-v1"; minLength=1; maxLength=4096; pattern="^[^/\\\\]+(?:/[^/\\\\]+)*$"; allOf=additional keys=`not` \| additional keys=`not`; additional keys=`x-unicode-normalization` |
| <a id="s-e440dc7980"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-221613439d"></a>`RetrievalFileReferenceDocument` | type="object"; fields=`collection_id`, `path`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.RetrievalFileReferenceSetDocument.validate_exact_reference_set](riverhog-protocol-retrievalfilereferencesetdocument-validate-exact-reference-set.md)

## Governing policies

- <a id="pa-5fe3c33ae8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RetrievalFileReferenceSetDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d13ccba69aabacc6c2ab59be40adf6bb3bb453e42a05f36127eef9c60b298bd5 -->

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
        "RetrievalFileReferenceDocument": {
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
          "title": "RetrievalFileReferenceDocument",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "files": {
          "items": {
            "$ref": "#/$defs/RetrievalFileReferenceDocument"
          },
          "maxItems": 10000,
          "minItems": 1,
          "title": "Files",
          "type": "array",
          "x-riverhog-extent": {
            "policy": "segmented_no_total_max",
            "progression": "multiple-retrieval-jobs",
            "reason": "bounded-retrieval-work-request"
          }
        }
      },
      "required": [
        "files"
      ],
      "title": "RetrievalFileReferenceSetDocument",
      "type": "object"
    },
    "signature": "'(*, files: Annotated[list[riverhog_protocol.retrieval_transport.RetrievalFileReferenceDocument], MinLen(min_length=1), MaxLen(max_length=10000)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RetrievalFileReferenceSetDocument",
  "unit": "export"
}
```
