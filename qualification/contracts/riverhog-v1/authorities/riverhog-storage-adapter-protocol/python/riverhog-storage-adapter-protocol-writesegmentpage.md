# riverhog_storage_adapter_protocol.WriteSegmentPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writesegmentpage:b29423904d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4d3f473c69"></a>
- <a id="s-887a4dc881"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-501165fd62"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-8d0c4f204c"></a>`name`: `WriteSegmentPage`
- <a id="s-b8ef02192f"></a>`unit`: `export`

### Declared structure

- <a id="s-41c34a8dd5"></a>`kind`: `"class"`
- <a id="s-e1a22ef083"></a>`signature`: `"'(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, traversal_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)], segments: Annotated[tuple[riverhog_storage_adapter_protocol.protocol.WriteSegmentReceipt, ...], MaxLen(max_length=128)] = (), next_after_number: Annotated[int \| None, Ge(ge=1)] = None, completion: riverhog_storage_adapter_protocol.protocol.WriteCompletionAuthority \| None = None) -> None'"`

#### Validated model schema

<a id="s-1247f2a421"></a>
- <a id="s-903688c0e3"></a>`title`: WriteSegmentPage
- <a id="s-2e1bde2946"></a>`description`: One bounded page under an adapter-owned immutable traversal view.
- <a id="s-4fd02c051b"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-00b7280fd0"></a>`completion` | no | anyOf=#/$defs/WriteCompletionAuthority \| type="null" |  |
| <a id="s-9930afc595"></a>`next_after_number` | no | anyOf=type="integer"; minimum=1 \| type="null" |  |
| <a id="s-c7cb242b01"></a>`segments` | no | type="array"; maxItems=128; items=(#/$defs/WriteSegmentReceipt); additional keys=`x-riverhog-extent` |  |
| <a id="s-19c41d3e62"></a>`session` | yes | #/$defs/WriteSession |  |
| <a id="s-a60eb0650f"></a>`traversal_token` | yes | type="string"; minLength=1; maxLength=4000 |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-bdd5ecbe60"></a>`WriteCompletionAuthority` | type="object"; fields=`authority_token`, `segment_count`, `stored_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-f1c59d947a"></a>`WriteSegmentReceipt` | type="object"; fields=`number`, `segment_token`, `stored_bytes`, `stored_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-3303effcc4"></a>`WriteSession` | type="object"; fields=`expected_bytes`, `object_path`, `write_token`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.WriteSegmentPage.canonical_segments](riverhog-storage-adapter-protocol-writesegmentpage-canonical-segments.md)
- [riverhog_storage_adapter_protocol.WriteSegmentPage.validate_terminal](riverhog-storage-adapter-protocol-writesegmentpage-validate-terminal.md)

## Governing policies

- <a id="pa-86b3b91db0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteSegmentPage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4ce4d67358554a16eda82ff4c964463c61a5475529e88f812658acc6b2066157 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "WriteCompletionAuthority": {
          "additionalProperties": false,
          "description": "Adapter-issued terminal authority for one exact active-write state.\n\nConsumers echo the opaque token unchanged. It is neither a credential nor a\nbearer capability; completion remains independently authorized. Once an exact\nimmutable object is published, its completed-object identity supersedes this\ntransport authority for terminal reconciliation.",
          "properties": {
            "authority_token": {
              "description": "Bounded opaque adapter-issued authority for the exact accepted state of an active write. The token grants no authority and must be echoed unchanged.",
              "maxLength": 4000,
              "minLength": 1,
              "title": "Authority Token",
              "type": "string"
            },
            "segment_count": {
              "minimum": 0,
              "title": "Segment Count",
              "type": "integer"
            },
            "stored_bytes": {
              "minimum": 0,
              "title": "Stored Bytes",
              "type": "integer"
            }
          },
          "required": [
            "segment_count",
            "stored_bytes",
            "authority_token"
          ],
          "title": "WriteCompletionAuthority",
          "type": "object"
        },
        "WriteSegmentReceipt": {
          "additionalProperties": false,
          "properties": {
            "number": {
              "minimum": 1,
              "title": "Number",
              "type": "integer"
            },
            "segment_token": {
              "maxLength": 4000,
              "minLength": 1,
              "title": "Segment Token",
              "type": "string"
            },
            "stored_bytes": {
              "minimum": 1,
              "title": "Stored Bytes",
              "type": "integer"
            },
            "stored_sha256": {
              "anyOf": [
                {
                  "pattern": "^[0-9a-f]{64}$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null,
              "title": "Stored Sha256"
            }
          },
          "required": [
            "number",
            "segment_token",
            "stored_bytes"
          ],
          "title": "WriteSegmentReceipt",
          "type": "object"
        },
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
      "description": "One bounded page under an adapter-owned immutable traversal view.",
      "properties": {
        "completion": {
          "anyOf": [
            {
              "$ref": "#/$defs/WriteCompletionAuthority"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "next_after_number": {
          "anyOf": [
            {
              "minimum": 1,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Next After Number"
        },
        "segments": {
          "default": [],
          "items": {
            "$ref": "#/$defs/WriteSegmentReceipt"
          },
          "maxItems": 128,
          "title": "Segments",
          "type": "array",
          "x-riverhog-extent": {
            "policy": "segmented_no_total_max",
            "progression": "exact-adapter-write-traversal",
            "reason": "bounded-storage-write-segment-page"
          }
        },
        "session": {
          "$ref": "#/$defs/WriteSession"
        },
        "traversal_token": {
          "maxLength": 4000,
          "minLength": 1,
          "title": "Traversal Token",
          "type": "string"
        }
      },
      "required": [
        "session",
        "traversal_token"
      ],
      "title": "WriteSegmentPage",
      "type": "object"
    },
    "signature": "'(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, traversal_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4000)], segments: Annotated[tuple[riverhog_storage_adapter_protocol.protocol.WriteSegmentReceipt, ...], MaxLen(max_length=128)] = (), next_after_number: Annotated[int | None, Ge(ge=1)] = None, completion: riverhog_storage_adapter_protocol.protocol.WriteCompletionAuthority | None = None) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteSegmentPage",
  "unit": "export"
}
```
