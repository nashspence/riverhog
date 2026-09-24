# riverhog_storage_adapter_protocol.WriteStartRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writestartrequest:5e161bc919 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1d2597b3cb"></a>
- <a id="s-6efb6d803a"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-48acf396cf"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-5ee1a04418"></a>`name`: `WriteStartRequest`
- <a id="s-c445dfbb70"></a>`unit`: `export`

### Declared structure

- <a id="s-7dbcaedcab"></a>`kind`: `"class"`
- <a id="s-66089900c1"></a>`signature`: `"\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], expected_bytes: PositiveDecimal, content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], placement_policy: Literal['archive_default', 'immediate_default']) -> None\""`

#### Validated model schema

<a id="s-5718ecb230"></a>

- <a id="s-ca9f1d8fd4"></a>`type`: `"object"`
- <a id="s-85823fc036"></a>`additionalProperties`: `false`
- <a id="s-389d33dd35"></a>`required`: `["object_path","expected_bytes","content_type","required_identity_assertions","placement_policy"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-66562d712e"></a>`content_type` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-0920fe8051"></a>`expected_bytes` | yes | [PositiveDecimal](#s-412bc07bda) |  |
| <a id="s-cb6fed8c86"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-2b5a564874"></a>`placement_policy` | yes | type="string"; enum=["archive_default","immediate_default"] |  |
| <a id="s-f32bac3861"></a>`required_identity_assertions` | yes | type="object"; additionalProperties=(type="string"); maxProperties=64; x-riverhog-encoded-bytes-max=16384; x-riverhog-extent={"policy":"contract_max","reason":"bounded-object-identity-assertion-envelope"} |  |

##### Definitions

- [PositiveDecimal](#s-412bc07bda)

##### <a id="s-412bc07bda"></a>definition `PositiveDecimal`

- <a id="s-2937c4df52"></a>`type`: `"string"`
- <a id="s-8da74a42e0"></a>`pattern`: `"^[1-9][0-9]*(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [canonical_metadata](riverhog-storage-adapter-protocol-writestartrequest-canonical-metadata.md)
- [canonical_path](riverhog-storage-adapter-protocol-writestartrequest-canonical-path.md)

## Governing policies

- <a id="pa-45ee3a3d90"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteStartRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 99587903d6bb85e6ae43893b3c813fa1c956761ebf27bfab4c9e1bd5f799763c -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "PositiveDecimal": {
          "pattern": "^[1-9][0-9]*(?![\\s\\S])",
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "content_type": {
          "maxLength": 255,
          "minLength": 1,
          "type": "string"
        },
        "expected_bytes": {
          "$ref": "#/$defs/PositiveDecimal"
        },
        "object_path": {
          "maxLength": 4096,
          "minLength": 1,
          "type": "string"
        },
        "placement_policy": {
          "enum": [
            "archive_default",
            "immediate_default"
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
        }
      },
      "required": [
        "object_path",
        "expected_bytes",
        "content_type",
        "required_identity_assertions",
        "placement_policy"
      ],
      "type": "object"
    },
    "signature": "\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], expected_bytes: PositiveDecimal, content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], placement_policy: Literal['archive_default', 'immediate_default']) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteStartRequest",
  "unit": "export"
}
```

</details>
