# riverhog_storage_adapter_protocol.WriteCompleteRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writeco-8bc312f21c:71bdb9f0ff -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-50ef59c596"></a>
- <a id="s-6f63e1d2ad"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-b543a074cb"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-779426bbd9"></a>`name`: `WriteCompleteRequest`
- <a id="s-0ac38a7301"></a>`unit`: `export`

### Declared structure

- <a id="s-9680c53b70"></a>`kind`: `"class"`
- <a id="s-e41639107b"></a>`signature`: `"\"(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, completion: riverhog_storage_adapter_protocol.protocol.WriteCompletionAuthority, expected_bytes: Annotated[int, Ge(ge=1)], expected_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], expected_placement: Literal['archive', 'immediate']) -> None\""`

#### Validated model schema

<a id="s-c446ae64ca"></a>
- <a id="s-9702328bbb"></a>`title`: WriteCompleteRequest
- <a id="s-149060f9b6"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-760d87722b"></a>`completion` | yes | #/$defs/WriteCompletionAuthority |  |
| <a id="s-7a8d87fa71"></a>`expected_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-ef006a81c9"></a>`expected_content_type` | yes | type="string"; minLength=1; maxLength=255 |  |
| <a id="s-5e42590d31"></a>`expected_placement` | yes | type="string"; enum=["archive","immediate"] |  |
| <a id="s-8c917da295"></a>`required_identity_assertions` | yes | type="object"; additional keys=`additionalProperties`, `maxProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` | Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions. |
| <a id="s-77b14e55b9"></a>`session` | yes | #/$defs/WriteSession |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-d78eb0b56c"></a>`WriteCompletionAuthority` | type="object"; fields=`authority_token`, `segment_count`, `stored_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-7089c76a7b"></a>`WriteSession` | type="object"; fields=`expected_bytes`, `object_path`, `write_token`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.WriteCompleteRequest.canonical_metadata](riverhog-storage-adapter-protocol-writecompleterequest-canonical-metadata.md)
- [riverhog_storage_adapter_protocol.WriteCompleteRequest.validate_bytes](riverhog-storage-adapter-protocol-writecompleterequest-validate-bytes.md)

## Governing policies

- <a id="pa-e6626ae0c9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteCompleteRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8a64191bf68d5e6ee50cf50274e1086b56dd0289253c65d60bfe8ec3c552b383 -->

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
      "properties": {
        "completion": {
          "$ref": "#/$defs/WriteCompletionAuthority"
        },
        "expected_bytes": {
          "minimum": 1,
          "title": "Expected Bytes",
          "type": "integer"
        },
        "expected_content_type": {
          "maxLength": 255,
          "minLength": 1,
          "title": "Expected Content Type",
          "type": "string"
        },
        "expected_placement": {
          "enum": [
            "archive",
            "immediate"
          ],
          "title": "Expected Placement",
          "type": "string"
        },
        "required_identity_assertions": {
          "additionalProperties": {
            "type": "string"
          },
          "description": "Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions.",
          "maxProperties": 64,
          "title": "Required Identity Assertions",
          "type": "object",
          "x-riverhog-encoded-bytes-max": 16384,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-object-identity-assertion-envelope"
          }
        },
        "session": {
          "$ref": "#/$defs/WriteSession"
        }
      },
      "required": [
        "session",
        "completion",
        "expected_bytes",
        "expected_content_type",
        "required_identity_assertions",
        "expected_placement"
      ],
      "title": "WriteCompleteRequest",
      "type": "object"
    },
    "signature": "\"(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, completion: riverhog_storage_adapter_protocol.protocol.WriteCompletionAuthority, expected_bytes: Annotated[int, Ge(ge=1)], expected_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], expected_placement: Literal['archive', 'immediate']) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteCompleteRequest",
  "unit": "export"
}
```
