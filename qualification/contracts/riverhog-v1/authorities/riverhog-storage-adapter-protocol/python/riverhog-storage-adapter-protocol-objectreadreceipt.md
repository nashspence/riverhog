# riverhog_storage_adapter_protocol.ObjectReadReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectreadreceipt:f0fbaa1083 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cc85bc1f0e"></a>
- <a id="s-4e5ba58d59"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-f3b568daec"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-3843e6cda2"></a>`name`: `ObjectReadReceipt`
- <a id="s-a930d0f1cf"></a>`unit`: `export`

### Declared structure

- <a id="s-0dae936377"></a>`kind`: `"class"`
- <a id="s-147b5a8af6"></a>`signature`: `"'(*, object: riverhog_storage_adapter_protocol.protocol.ObjectLocator, total_bytes: NonnegativeDecimal, offset: NonnegativeDecimal, read_bytes: NonnegativeDecimal) -> None'"`

#### Validated model schema

<a id="s-2b4e9c5bd7"></a>

- <a id="s-63f5762c25"></a>`type`: `"object"`
- <a id="s-2210d51a58"></a>`additionalProperties`: `false`
- <a id="s-eeff13e697"></a>`required`: `["object","total_bytes","offset","read_bytes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3feef6f54a"></a>`object` | yes | [ObjectLocator](#s-4d487770c6) |  |
| <a id="s-897ac44be0"></a>`offset` | yes | [NonnegativeDecimal](#s-c58b8e0d85) |  |
| <a id="s-db424d7fdd"></a>`read_bytes` | yes | [NonnegativeDecimal](#s-c58b8e0d85) |  |
| <a id="s-e930bb066b"></a>`total_bytes` | yes | [NonnegativeDecimal](#s-c58b8e0d85) |  |

##### Definitions

- [NonnegativeDecimal](#s-c58b8e0d85)
- [ObjectLocator](#s-4d487770c6)

##### <a id="s-c58b8e0d85"></a>definition `NonnegativeDecimal`

- <a id="s-4c30e57ae3"></a>`type`: `"string"`
- <a id="s-2ffc7cae7a"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-4d487770c6"></a>definition `ObjectLocator`

- <a id="s-abe92aaac1"></a>`type`: `"object"`
- <a id="s-ba0be28e56"></a>`additionalProperties`: `false`
- <a id="s-b13037cf4a"></a>`required`: `["object_path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c9b9355869"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-dec2a2d4e2"></a>`revision` | no | anyOf=[(type="string"; maxLength=2000; minLength=1); (type="null")]; default=null |  |

## Maintained corroboration

### Related interface records

- [validate_range](riverhog-storage-adapter-protocol-objectreadreceipt-validate-range.md)

## Governing policies

- <a id="pa-cc7323be97"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectReadReceipt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ce4d2dc228f17f529e4adda1c10d7dc201549f2d89a9f357edb68bf4e6cd4681 -->

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
        "object": {
          "$ref": "#/$defs/ObjectLocator"
        },
        "offset": {
          "$ref": "#/$defs/NonnegativeDecimal"
        },
        "read_bytes": {
          "$ref": "#/$defs/NonnegativeDecimal"
        },
        "total_bytes": {
          "$ref": "#/$defs/NonnegativeDecimal"
        }
      },
      "required": [
        "object",
        "total_bytes",
        "offset",
        "read_bytes"
      ],
      "type": "object"
    },
    "signature": "'(*, object: riverhog_storage_adapter_protocol.protocol.ObjectLocator, total_bytes: NonnegativeDecimal, offset: NonnegativeDecimal, read_bytes: NonnegativeDecimal) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ObjectReadReceipt",
  "unit": "export"
}
```

</details>
