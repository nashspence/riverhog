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

- <a id="s-4fd02c051b"></a>`type`: `"object"`
- <a id="s-7e117267ac"></a>`additionalProperties`: `false`
- <a id="s-7610aac3b8"></a>`required`: `["session","traversal_token"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-00b7280fd0"></a>`completion` | no | anyOf=[([WriteCompletionAuthority](#s-bdd5ecbe60)); (type="null")]; default=null |  |
| <a id="s-9930afc595"></a>`next_after_number` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null |  |
| <a id="s-c7cb242b01"></a>`segments` | no | type="array"; default=[]; items=([WriteSegmentReceipt](#s-f1c59d947a)); maxItems=128; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"exact-adapter-write-traversal","reason":"bounded-storage-write-segment-page"} |  |
| <a id="s-19c41d3e62"></a>`session` | yes | [WriteSession](#s-3303effcc4) |  |
| <a id="s-a60eb0650f"></a>`traversal_token` | yes | type="string"; maxLength=4000; minLength=1 |  |

##### Definitions

- [WriteCompletionAuthority](#s-bdd5ecbe60)
- [WriteSegmentReceipt](#s-f1c59d947a)
- [WriteSession](#s-3303effcc4)

##### <a id="s-bdd5ecbe60"></a>definition `WriteCompletionAuthority`

- <a id="s-6445d09d2a"></a>`type`: `"object"`
- <a id="s-deab48a17a"></a>`additionalProperties`: `false`
- <a id="s-4c7ac3b353"></a>`required`: `["segment_count","stored_bytes","authority_token"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c412c0cfe6"></a>`authority_token` | yes | type="string"; maxLength=4000; minLength=1 |  |
| <a id="s-93343dc83f"></a>`segment_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-45b6a6bed2"></a>`stored_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-f1c59d947a"></a>definition `WriteSegmentReceipt`

- <a id="s-c9dfeee29d"></a>`type`: `"object"`
- <a id="s-3b19b017a2"></a>`additionalProperties`: `false`
- <a id="s-e8511a7384"></a>`required`: `["number","segment_token","stored_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-04a6cbda43"></a>`number` | yes | type="integer"; minimum=1 |  |
| <a id="s-16e346847c"></a>`segment_token` | yes | type="string"; maxLength=4000; minLength=1 |  |
| <a id="s-5d370e6e2f"></a>`stored_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-ed65f66ca2"></a>`stored_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |

##### <a id="s-3303effcc4"></a>definition `WriteSession`

- <a id="s-f65b861220"></a>`type`: `"object"`
- <a id="s-99d200198a"></a>`additionalProperties`: `false`
- <a id="s-f2f1288029"></a>`required`: `["object_path","expected_bytes","write_token"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3d82c6f0a9"></a>`expected_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-f80ef4dafd"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-d40cc4ae9e"></a>`write_token` | yes | type="string"; maxLength=4000; minLength=1 |  |

## Maintained corroboration

### Related interface records

- [canonical_segments](riverhog-storage-adapter-protocol-writesegmentpage-canonical-segments.md)
- [validate_terminal](riverhog-storage-adapter-protocol-writesegmentpage-validate-terminal.md)

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

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc907e3ab5e960db95c573aa18b1cb8a99006d44782b4226127a9a87cb1c7b22 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "WriteCompletionAuthority": {
          "additionalProperties": false,
          "properties": {
            "authority_token": {
              "maxLength": 4000,
              "minLength": 1,
              "type": "string"
            },
            "segment_count": {
              "minimum": 0,
              "type": "integer"
            },
            "stored_bytes": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "segment_count",
            "stored_bytes",
            "authority_token"
          ],
          "type": "object"
        },
        "WriteSegmentReceipt": {
          "additionalProperties": false,
          "properties": {
            "number": {
              "minimum": 1,
              "type": "integer"
            },
            "segment_token": {
              "maxLength": 4000,
              "minLength": 1,
              "type": "string"
            },
            "stored_bytes": {
              "minimum": 1,
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
              "default": null
            }
          },
          "required": [
            "number",
            "segment_token",
            "stored_bytes"
          ],
          "type": "object"
        },
        "WriteSession": {
          "additionalProperties": false,
          "properties": {
            "expected_bytes": {
              "minimum": 1,
              "type": "integer"
            },
            "object_path": {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            },
            "write_token": {
              "maxLength": 4000,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "object_path",
            "expected_bytes",
            "write_token"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
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
          "default": null
        },
        "segments": {
          "default": [],
          "items": {
            "$ref": "#/$defs/WriteSegmentReceipt"
          },
          "maxItems": 128,
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
          "type": "string"
        }
      },
      "required": [
        "session",
        "traversal_token"
      ],
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

</details>
