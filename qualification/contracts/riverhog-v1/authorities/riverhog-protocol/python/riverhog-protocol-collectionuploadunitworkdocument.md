# riverhog_protocol.CollectionUploadUnitWorkDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadunitworkdocument:5f3207bf12 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dab1662726"></a>
- <a id="s-2e33e51c02"></a>`distribution`: `riverhog-protocol`
- <a id="s-b98beb79bc"></a>`module`: `riverhog_protocol`
- <a id="s-ae8145032e"></a>`name`: `CollectionUploadUnitWorkDocument`
- <a id="s-e700e9fd05"></a>`unit`: `export`

### Declared structure

- <a id="s-200c736c5f"></a>`kind`: `"class"`
- <a id="s-1717b69867"></a>`signature`: `"\"(*, unit: Annotated[int, Ge(ge=0)], payload_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], plaintext_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], sources: Annotated[list[riverhog_protocol.collection_upload_transport.CollectionUploadUnitSourceDocument], MaxLen(max_length=1000)], state: Literal['pending', 'committed']) -> None\""`

#### Validated model schema

<a id="s-525cc598ba"></a>
- <a id="s-9b92b950d9"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f093d62ec0"></a>`payload_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-04dad5124f"></a>`plaintext_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-a58e9ea803"></a>`sources` | yes | type="array"; maxItems=1000; items=(#/$defs/CollectionUploadUnitSourceDocument); additional keys=`x-riverhog-extent` |  |
| <a id="s-bd038539c2"></a>`state` | yes | type="string"; enum=["pending","committed"] |  |
| <a id="s-e998ea77d3"></a>`unit` | yes | type="integer"; minimum=0 |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-daf8b1a175"></a>`CollectionUploadUnitSourceDocument` | type="object"; fields=`artifact_sha256`, `bytes`, `offset`, `path`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-2918ee7ee4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadUnitWorkDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e3c6c3703efaf28ac07ef9220ce1cf4969b2d5f44ed97da99b7ee868cc70fe5b -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionUploadUnitSourceDocument": {
          "additionalProperties": false,
          "properties": {
            "artifact_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "offset": {
              "minimum": 0,
              "type": "integer"
            },
            "path": {
              "type": "string"
            }
          },
          "required": [
            "path",
            "offset",
            "bytes",
            "artifact_sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "payload_bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "plaintext_bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "sources": {
          "items": {
            "$ref": "#/$defs/CollectionUploadUnitSourceDocument"
          },
          "maxItems": 1000,
          "type": "array",
          "x-riverhog-extent": {
            "policy": "segmented_no_total_max",
            "progression": "collection-volume-sequence",
            "reason": "bounded-upload-unit-source-map"
          }
        },
        "state": {
          "enum": [
            "pending",
            "committed"
          ],
          "type": "string"
        },
        "unit": {
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "unit",
        "payload_bytes",
        "plaintext_bytes",
        "sources",
        "state"
      ],
      "type": "object"
    },
    "signature": "\"(*, unit: Annotated[int, Ge(ge=0)], payload_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], plaintext_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], sources: Annotated[list[riverhog_protocol.collection_upload_transport.CollectionUploadUnitSourceDocument], MaxLen(max_length=1000)], state: Literal['pending', 'committed']) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadUnitWorkDocument",
  "unit": "export"
}
```
