# riverhog_storage_adapter_protocol.WriteCompleteRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writeco-8bc312f21c:71bdb9f0ff -->

Exact externally visible contract owned by this contract element.

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
- <a id="s-e41639107b"></a>`signature`: `"\"(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, completion: riverhog_storage_adapter_protocol.protocol.WriteCompletionPrecondition, expected_bytes: PositiveDecimal, expected_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], expected_placement: Literal['archive', 'immediate']) -> None\""`

#### Validated model schema

<a id="s-c446ae64ca"></a>

- <a id="s-149060f9b6"></a>`type`: `"object"`
- <a id="s-06c4ee0293"></a>`additionalProperties`: `false`
- <a id="s-0c8939af9a"></a>`required`: `["session","completion","expected_bytes","expected_content_type","required_identity_assertions","expected_placement"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-760d87722b"></a>`completion` | yes | [WriteCompletionPrecondition](#s-a37b597897) |  |
| <a id="s-7a8d87fa71"></a>`expected_bytes` | yes | [PositiveDecimal](#s-be642acd83) |  |
| <a id="s-ef006a81c9"></a>`expected_content_type` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-5e42590d31"></a>`expected_placement` | yes | type="string"; enum=["archive","immediate"] |  |
| <a id="s-8c917da295"></a>`required_identity_assertions` | yes | type="object"; additionalProperties=(type="string"); maxProperties=64; x-riverhog-encoded-bytes-max=16384; x-riverhog-extent={"policy":"contract_max","reason":"bounded-object-identity-assertion-envelope"} |  |
| <a id="s-77b14e55b9"></a>`session` | yes | [WriteSession](#s-7089c76a7b) |  |

##### Definitions

- [NonnegativeDecimal](#s-1fc0650e04)
- [PositiveDecimal](#s-be642acd83)
- [WriteCompletionPrecondition](#s-a37b597897)
- [WriteSession](#s-7089c76a7b)

##### <a id="s-1fc0650e04"></a>definition `NonnegativeDecimal`

- <a id="s-0e0b9bc7d9"></a>`type`: `"string"`
- <a id="s-00a72379cf"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-be642acd83"></a>definition `PositiveDecimal`

- <a id="s-13456a80f7"></a>`type`: `"string"`
- <a id="s-018ca4b4b8"></a>`pattern`: `"^[1-9][0-9]*(?![\\s\\S])"`

##### <a id="s-a37b597897"></a>definition `WriteCompletionPrecondition`

- <a id="s-14c4df4869"></a>`type`: `"object"`
- <a id="s-5820bce365"></a>`additionalProperties`: `false`
- <a id="s-2e6515efb8"></a>`required`: `["segment_count","stored_bytes","state_token"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9542f337eb"></a>`segment_count` | yes | [NonnegativeDecimal](#s-1fc0650e04) |  |
| <a id="s-b83cb456d1"></a>`state_token` | yes | type="string"; maxLength=4000; minLength=1 |  |
| <a id="s-614940ce20"></a>`stored_bytes` | yes | [NonnegativeDecimal](#s-1fc0650e04) |  |

##### <a id="s-7089c76a7b"></a>definition `WriteSession`

- <a id="s-57286e4b07"></a>`type`: `"object"`
- <a id="s-3526c9de9a"></a>`additionalProperties`: `false`
- <a id="s-7a8021ab37"></a>`required`: `["object_path","expected_bytes","write_token"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e029c297cf"></a>`expected_bytes` | yes | [PositiveDecimal](#s-be642acd83) |  |
| <a id="s-3af3650aa5"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-4a9ee9c2c9"></a>`write_token` | yes | type="string"; maxLength=4000; minLength=1 |  |

## Maintained corroboration

### Related interface records

- [canonical_metadata](riverhog-storage-adapter-protocol-writecompleterequest-canonical-metadata.md)
- [validate_bytes](riverhog-storage-adapter-protocol-writecompleterequest-validate-bytes.md)

## Governing policies

- <a id="pa-e6626ae0c9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteCompleteRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3fb294a8e399803571814e5360ad3b41a398eb1641ea1a79e891c437c9ea513d -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        },
        "PositiveDecimal": {
          "pattern": "^[1-9][0-9]*(?![\\s\\S])",
          "type": "string"
        },
        "WriteCompletionPrecondition": {
          "additionalProperties": false,
          "properties": {
            "segment_count": {
              "$ref": "#/$defs/NonnegativeDecimal"
            },
            "state_token": {
              "maxLength": 4000,
              "minLength": 1,
              "type": "string"
            },
            "stored_bytes": {
              "$ref": "#/$defs/NonnegativeDecimal"
            }
          },
          "required": [
            "segment_count",
            "stored_bytes",
            "state_token"
          ],
          "type": "object"
        },
        "WriteSession": {
          "additionalProperties": false,
          "properties": {
            "expected_bytes": {
              "$ref": "#/$defs/PositiveDecimal"
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
          "$ref": "#/$defs/WriteCompletionPrecondition"
        },
        "expected_bytes": {
          "$ref": "#/$defs/PositiveDecimal"
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
    "signature": "\"(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, completion: riverhog_storage_adapter_protocol.protocol.WriteCompletionPrecondition, expected_bytes: PositiveDecimal, expected_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], expected_placement: Literal['archive', 'immediate']) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteCompleteRequest",
  "unit": "export"
}
```

</details>
