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

- <a id="s-149060f9b6"></a>`type`: `"object"`
- <a id="s-06c4ee0293"></a>`additionalProperties`: `false`
- <a id="s-0c8939af9a"></a>`required`: `["session","completion","expected_bytes","expected_content_type","required_identity_assertions","expected_placement"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-760d87722b"></a>`completion` | yes | [WriteCompletionAuthority](#s-d78eb0b56c) |  |
| <a id="s-7a8d87fa71"></a>`expected_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-ef006a81c9"></a>`expected_content_type` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-5e42590d31"></a>`expected_placement` | yes | type="string"; enum=["archive","immediate"] |  |
| <a id="s-8c917da295"></a>`required_identity_assertions` | yes | type="object"; additionalProperties=(type="string"); maxProperties=64; x-riverhog-encoded-bytes-max=16384; x-riverhog-extent={"policy":"contract_max","reason":"bounded-object-identity-assertion-envelope"} |  |
| <a id="s-77b14e55b9"></a>`session` | yes | [WriteSession](#s-7089c76a7b) |  |

##### Definitions

- [WriteCompletionAuthority](#s-d78eb0b56c)
- [WriteSession](#s-7089c76a7b)

##### <a id="s-d78eb0b56c"></a>definition `WriteCompletionAuthority`

- <a id="s-bbd2a5fc47"></a>`type`: `"object"`
- <a id="s-0fa21b80f6"></a>`additionalProperties`: `false`
- <a id="s-03ecbf432b"></a>`required`: `["segment_count","stored_bytes","authority_token"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3197417b17"></a>`authority_token` | yes | type="string"; maxLength=4000; minLength=1 |  |
| <a id="s-75cf91dbd4"></a>`segment_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-2bd6b08275"></a>`stored_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-7089c76a7b"></a>definition `WriteSession`

- <a id="s-57286e4b07"></a>`type`: `"object"`
- <a id="s-3526c9de9a"></a>`additionalProperties`: `false`
- <a id="s-7a8021ab37"></a>`required`: `["object_path","expected_bytes","write_token"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e029c297cf"></a>`expected_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-3af3650aa5"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-4a9ee9c2c9"></a>`write_token` | yes | type="string"; maxLength=4000; minLength=1 |  |

## Maintained corroboration

### Related interface records

- [canonical_metadata](riverhog-storage-adapter-protocol-writecompleterequest-canonical-metadata.md)
- [validate_bytes](riverhog-storage-adapter-protocol-writecompleterequest-validate-bytes.md)

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

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7253b1030d678c8d5c0ed35a3bc411614a58364f9cbf127e679e5318f6fe1706 -->

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
          "$ref": "#/$defs/WriteCompletionAuthority"
        },
        "expected_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "expected_content_type": {
          "maxLength": 255,
          "minLength": 1,
          "type": "string"
        },
        "expected_placement": {
          "enum": [
            "archive",
            "immediate"
          ],
          "type": "string"
        },
        "required_identity_assertions": {
          "additionalProperties": {
            "type": "string"
          },
          "maxProperties": 64,
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

</details>
