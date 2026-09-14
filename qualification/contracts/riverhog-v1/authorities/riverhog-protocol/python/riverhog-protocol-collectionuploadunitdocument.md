# riverhog_protocol.CollectionUploadUnitDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadunitdocument:6830ef1875 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-203eb98a36"></a>
- <a id="s-8567377bf2"></a>`distribution`: `riverhog-protocol`
- <a id="s-6dc76405e5"></a>`module`: `riverhog_protocol`
- <a id="s-92121b4355"></a>`name`: `CollectionUploadUnitDocument`
- <a id="s-05ba1be1a8"></a>`unit`: `export`

### Declared structure

- <a id="s-7739825a4e"></a>`kind`: `"class"`
- <a id="s-5d2ef7790e"></a>`signature`: `"'(*, unit: Annotated[int, Ge(ge=0)], payload_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], plaintext_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], sources: Annotated[list[riverhog_protocol.collection_upload_transport.CollectionUploadUnitSourceDocument], MaxLen(max_length=1000)]) -> None'"`

#### Validated model schema

<a id="s-2718db9f9d"></a>
- <a id="s-b2ba19c19e"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e4325beb51"></a>`payload_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-ec15f49a7a"></a>`plaintext_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-951531c70a"></a>`sources` | yes | type="array"; maxItems=1000; items=(#/$defs/CollectionUploadUnitSourceDocument); additional keys=`x-riverhog-extent` |  |
| <a id="s-b5b9e2d464"></a>`unit` | yes | type="integer"; minimum=0 |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-802970dc2d"></a>`CollectionUploadUnitSourceDocument` | type="object"; fields=`artifact_sha256`, `bytes`, `offset`, `path`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [validate_sources](riverhog-protocol-collectionuploadunitdocument-validate-sources.md)

## Governing policies

- <a id="pa-1ce1353a3e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadUnitDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2d1a45bee31f97d004368c6aa2cb0232298bd0744a762b7a9391ca321f20f41e -->

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
        "unit": {
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "unit",
        "payload_bytes",
        "plaintext_bytes",
        "sources"
      ],
      "type": "object"
    },
    "signature": "'(*, unit: Annotated[int, Ge(ge=0)], payload_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], plaintext_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], sources: Annotated[list[riverhog_protocol.collection_upload_transport.CollectionUploadUnitSourceDocument], MaxLen(max_length=1000)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadUnitDocument",
  "unit": "export"
}
```
