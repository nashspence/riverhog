# riverhog_storage_adapter_protocol.ObjectHeadRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectheadrequest:c864ad29b1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eef88604e6"></a>
- <a id="s-d83845eb3f"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-7785b73456"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-c7d8f054a9"></a>`name`: `ObjectHeadRequest`
- <a id="s-399b5cf29e"></a>`unit`: `export`

### Declared structure

- <a id="s-fb610e0881"></a>`kind`: `"class"`
- <a id="s-057720001d"></a>`signature`: `"\"(*, object: riverhog_storage_adapter_protocol.protocol.ObjectLocator, expected_placement_policy: Literal['archive_default', 'immediate_default']) -> None\""`

#### Validated model schema

<a id="s-70d5e9eea3"></a>

- <a id="s-e0ac15e6f5"></a>`type`: `"object"`
- <a id="s-e3a97b24c8"></a>`additionalProperties`: `false`
- <a id="s-cab2844872"></a>`required`: `["object","expected_placement_policy"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bce09d9b33"></a>`expected_placement_policy` | yes | type="string"; enum=["archive_default","immediate_default"] |  |
| <a id="s-01cf494aa8"></a>`object` | yes | [ObjectLocator](#s-6b8a5bc6a1) |  |

##### Definitions

- [ObjectLocator](#s-6b8a5bc6a1)

##### <a id="s-6b8a5bc6a1"></a>definition `ObjectLocator`

- <a id="s-e8714356b3"></a>`type`: `"object"`
- <a id="s-9e297301bc"></a>`additionalProperties`: `false`
- <a id="s-66f738fd4b"></a>`required`: `["object_path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cc7230a836"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-36f67bf238"></a>`revision` | no | anyOf=[(type="string"; maxLength=2000; minLength=1); (type="null")]; default=null |  |

## Governing policies

- <a id="pa-c5e98fc1b4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectHeadRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fe4827e6edccfe4cf8f4fbff8dddcdb569e00cb27ed316a35805abbc5f0cad8c -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ObjectLocator": {
          "additionalProperties": false,
          "properties": {
            "object_path": {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            },
            "revision": {
              "anyOf": [
                {
                  "maxLength": 2000,
                  "minLength": 1,
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
            "object_path"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "expected_placement_policy": {
          "enum": [
            "archive_default",
            "immediate_default"
          ],
          "type": "string"
        },
        "object": {
          "$ref": "#/$defs/ObjectLocator"
        }
      },
      "required": [
        "object",
        "expected_placement_policy"
      ],
      "type": "object"
    },
    "signature": "\"(*, object: riverhog_storage_adapter_protocol.protocol.ObjectLocator, expected_placement_policy: Literal['archive_default', 'immediate_default']) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ObjectHeadRequest",
  "unit": "export"
}
```

</details>
