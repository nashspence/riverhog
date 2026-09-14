# riverhog_protocol.ArchiveCopyStoreSelectionDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-archivecopystoreselectiondocument:2bbb4b7c7f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-30c690a6e0"></a>
- <a id="s-e3c19e20cd"></a>`distribution`: `riverhog-protocol`
- <a id="s-70ce011605"></a>`module`: `riverhog_protocol`
- <a id="s-5943d1a0e0"></a>`name`: `ArchiveCopyStoreSelectionDocument`
- <a id="s-f6a197f9d5"></a>`unit`: `export`

### Declared structure

- <a id="s-f3c6168d36"></a>`kind`: `"class"`
- <a id="s-07649ea83b"></a>`signature`: `"'(*, destination_store: ArchiveStoreName, source_store: ArchiveStoreName \| None = None) -> None'"`

#### Validated model schema

<a id="s-81572973e4"></a>
- <a id="s-ceffc5fb57"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-95af627683"></a>`destination_store` | yes | #/$defs/ArchiveStoreName |  |
| <a id="s-3ebeef1a2b"></a>`source_store` | no | anyOf=#/$defs/ArchiveStoreName \| type="null" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-49e2d5d755"></a>`ArchiveStoreName` | type="string"; pattern="^[a-z0-9]+(?:-[a-z0-9]+)*$" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.ArchiveCopyStoreSelectionDocument.validate_distinct_stores](riverhog-protocol-archivecopystoreselectiondocument-validate-distinct-stores.md)

## Governing policies

- <a id="pa-28b3131d1e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ArchiveCopyStoreSelectionDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 98aa99e0d864d6a4d186dbe953f394f2543dd2d7e4dccdbc8a70e69f477ced3b -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArchiveStoreName": {
          "pattern": "^[a-z0-9]+(?:-[a-z0-9]+)*$",
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "destination_store": {
          "$ref": "#/$defs/ArchiveStoreName"
        },
        "source_store": {
          "anyOf": [
            {
              "$ref": "#/$defs/ArchiveStoreName"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "required": [
        "destination_store"
      ],
      "type": "object"
    },
    "signature": "'(*, destination_store: ArchiveStoreName, source_store: ArchiveStoreName | None = None) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ArchiveCopyStoreSelectionDocument",
  "unit": "export"
}
```
