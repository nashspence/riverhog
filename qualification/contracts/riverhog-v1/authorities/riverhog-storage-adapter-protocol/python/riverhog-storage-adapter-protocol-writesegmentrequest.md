# riverhog_storage_adapter_protocol.WriteSegmentRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writese-46ea2c26f4:73e4c923d1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9919f8e86c"></a>
- <a id="s-07eef77820"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-685571e12f"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-d15cd24c47"></a>`name`: `WriteSegmentRequest`
- <a id="s-b7039396c9"></a>`unit`: `export`

### Declared structure

- <a id="s-ec5585b1f5"></a>`kind`: `"class"`
- <a id="s-f0c1b873f3"></a>`signature`: `"'(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, number: Annotated[int, Ge(ge=1)], stored_bytes: Annotated[int, Ge(ge=1)]) -> None'"`

#### Validated model schema

<a id="s-af6898c92c"></a>

- <a id="s-8b119c136e"></a>`type`: `"object"`
- <a id="s-3d7f79f0d0"></a>`additionalProperties`: `false`
- <a id="s-c2a507f4a9"></a>`required`: `["session","number","stored_bytes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-29ec31d434"></a>`number` | yes | type="integer"; minimum=1 |  |
| <a id="s-a92f6bcec1"></a>`session` | yes | [WriteSession](#s-3bcf6f4274) |  |
| <a id="s-bdfbfb086e"></a>`stored_bytes` | yes | type="integer"; minimum=1 |  |

##### Definitions

- [WriteSession](#s-3bcf6f4274)

##### <a id="s-3bcf6f4274"></a>definition `WriteSession`

- <a id="s-2e6103c361"></a>`type`: `"object"`
- <a id="s-c6f1f7b523"></a>`additionalProperties`: `false`
- <a id="s-005ad67ded"></a>`required`: `["object_path","expected_bytes","write_token"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fb09cd7729"></a>`expected_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-f42929e90e"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-9a5d226096"></a>`write_token` | yes | type="string"; maxLength=4000; minLength=1 |  |

## Governing policies

- <a id="pa-eecdcac42d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteSegmentRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cc8ae42250ef44a512468a9dc62c2c71a6a839e43fae790c28223acfd2a8dee0 -->

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
        "number": {
          "minimum": 1,
          "type": "integer"
        },
        "session": {
          "$ref": "#/$defs/WriteSession"
        },
        "stored_bytes": {
          "minimum": 1,
          "type": "integer"
        }
      },
      "required": [
        "session",
        "number",
        "stored_bytes"
      ],
      "type": "object"
    },
    "signature": "'(*, session: riverhog_storage_adapter_protocol.protocol.WriteSession, number: Annotated[int, Ge(ge=1)], stored_bytes: Annotated[int, Ge(ge=1)]) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "WriteSegmentRequest",
  "unit": "export"
}
```

</details>
