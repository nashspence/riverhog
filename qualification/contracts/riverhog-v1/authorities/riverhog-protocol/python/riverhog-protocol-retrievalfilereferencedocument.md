# riverhog_protocol.RetrievalFileReferenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retrievalfilereferencedocument:eb093ca910 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-af6a5277b0"></a>`title`: RetrievalFileReferenceDocument
- <a id="s-b897be87c8"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7be108cddd"></a>`collection_id` | yes | #/$defs/CollectionId |  |
| <a id="s-7e9ef988be"></a>`path` | yes | #/$defs/CanonicalRelPath |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-63a8c2807f"></a>`CanonicalRelPath` | type="string"; format="riverhog-canonical-relpath-v1"; minLength=1; maxLength=4096; pattern="^[^/\\\\]+(?:/[^/\\\\]+)*$"; allOf=additional keys=`not` \| additional keys=`not`; additional keys=`x-unicode-normalization` |
| <a id="s-66b4e9bd4e"></a>`CollectionId` | type="integer"; minimum=1 |

## Governing policies

- <a id="pa-f8c9fe0c85"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RetrievalFileReferenceDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 688daf5d3fd321d531eb0060a3b3321d15164989c99fd6374ab5dcb53661c273 -->

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
      "title": "RetrievalFileReferenceDocument",
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
