# riverhog_storage_adapter_protocol.WriteSegmentListRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writese-f8483c1c43:0df1f208f0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d3cbfbec1d"></a>
- <a id="s-aace5a99e0"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-930616bc27"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-13148a36ee"></a>`name`: `WriteSegmentListRequest`
- <a id="s-e990b6d6c0"></a>`unit`: `export`

### Declared structure

- <a id="s-e5633b8e7c"></a>`kind`: `"class"`
- <a id="s-cb3ab0dc9d"></a>`signature`: `"'(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, after_number: Annotated[int, Ge(ge=0)] = 0, traversal_token: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, maximum_items: Annotated[int, Ge(ge=1), Le(le=128)] = 128) -> None'"`

#### Validated model schema

<a id="s-bb9487fbdc"></a>
- <a id="s-bce0c4ce47"></a>`title`: WriteSegmentListRequest
- <a id="s-024ba13bf4"></a>`description`: Request one bounded page from an exact accepted-segment view.
- <a id="s-d723c95cd1"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9de7fb3114"></a>`after_number` | no | type="integer"; minimum=0; additional keys=`x-riverhog-extent` |  |
| <a id="s-271af5454e"></a>`maximum_items` | no | type="integer"; minimum=1; maximum=128 |  |
| <a id="s-3d7b574285"></a>`session` | yes | #/$defs/WriteSession |  |
| <a id="s-a54ca5cef3"></a>`traversal_token` | no | anyOf=type="string"; minLength=1; maxLength=4000 \| type="null" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-14ac5609c9"></a>`WriteSession` | type="object"; fields=`expected_bytes`, `object_path`, `write_token`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-3cb31a2dab"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteSegmentListRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e88766324ada0a773b711d7070983cb8a4a9de677f12c15e35d18200f446f614 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "WriteSession": {
          "additionalProperties": false,
          "properties": {
            "expected_bytes": {
              "description": "Exact immutable-object byte length admitted by this write session. The value remains fixed until the write becomes terminal.",
              "minimum": 1,
              "title": "Expected Bytes",
              "type": "integer"
            },
            "object_path": {
              "maxLength": 4096,
              "minLength": 1,
              "title": "Object Path",
              "type": "string"
            },
            "write_token": {
              "description": "Opaque adapter-owned persistable continuation handle. For the same configured adapter it remains replayable across client, transport, Riverhog, and adapter process restarts until completion, explicit abort, or caller-authorized incomplete-write reclamation makes the write terminal.",
              "maxLength": 4000,
              "minLength": 1,
              "title": "Write Token",
              "type": "string"
            }
          },
          "required": [
            "object_path",
            "expected_bytes",
            "write_token"
          ],
          "title": "WriteSession",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "description": "Request one bounded page from an exact accepted-segment view.",
      "properties": {
        "after_number": {
          "default": 0,
          "minimum": 0,
          "title": "After Number",
          "type": "integer",
          "x-riverhog-extent": {
            "policy": "segmented_no_total_max",
            "reason": "write-segment-history-bounded-traversal"
          }
        },
        "maximum_items": {
          "default": 128,
          "maximum": 128,
          "minimum": 1,
          "title": "Maximum Items",
          "type": "integer"
        },
        "session": {
          "$ref": "#/$defs/WriteSession"
        },
        "traversal_token": {
          "anyOf": [
            {
              "maxLength": 4000,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Traversal Token"
        }
      },
      "required": [
        "session"
      ],
      "title": "WriteSegmentListRequest",
      "type": "object"
    },
    "signature": "'(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, after_number: Annotated[int, Ge(ge=0)] = 0, traversal_token: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=4000)] = None, maximum_items: Annotated[int, Ge(ge=1), Le(le=128)] = 128) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteSegmentListRequest",
  "unit": "export"
}
```
