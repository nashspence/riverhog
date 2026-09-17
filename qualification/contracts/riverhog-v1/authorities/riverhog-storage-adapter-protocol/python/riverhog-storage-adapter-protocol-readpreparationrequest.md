# riverhog_storage_adapter_protocol.ReadPreparationRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-readpre-8dcef0ea20:79f93417b2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b46635c802"></a>
- <a id="s-4c45dd9185"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-403e0455b1"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-cc9088dc29"></a>`name`: `ReadPreparationRequest`
- <a id="s-f6561e4d4e"></a>`unit`: `export`

### Declared structure

- <a id="s-a19ce41695"></a>`kind`: `"class"`
- <a id="s-38b6dfe6d9"></a>`signature`: `"'(*, objects: Annotated[tuple[riverhog_storage_adapter_protocol.protocol.ObjectLocator, ...], MinLen(min_length=1)]) -> None'"`

#### Validated model schema

<a id="s-f734e423ba"></a>

- <a id="s-6cdd10672d"></a>`type`: `"object"`
- <a id="s-d8299ccc89"></a>`additionalProperties`: `false`
- <a id="s-dfbda94e5c"></a>`required`: `["objects"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-17287bf84c"></a>`objects` | yes | type="array"; items=([ObjectLocator](#s-e6685bce6c)); minItems=1 |  |

##### Definitions

- [ObjectLocator](#s-e6685bce6c)

##### <a id="s-e6685bce6c"></a>definition `ObjectLocator`

- <a id="s-50c128fa53"></a>`type`: `"object"`
- <a id="s-3c3699580f"></a>`additionalProperties`: `false`
- <a id="s-e63bb9f186"></a>`required`: `["object_path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c517db2ff3"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-b0aa2766b7"></a>`revision` | no | anyOf=[(type="string"; maxLength=2000; minLength=1); (type="null")]; default=null |  |

## Maintained corroboration

### Related interface records

- [canonical_objects](riverhog-storage-adapter-protocol-readpreparationrequest-canonical-objects.md)

## Governing policies

- <a id="pa-56884d6212"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ReadPreparationRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2d8360227feef6d98e7ab91f9bdebff8f352ab35cb585fa3c2917cc51860890d -->

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
        "objects": {
          "items": {
            "$ref": "#/$defs/ObjectLocator"
          },
          "minItems": 1,
          "type": "array"
        }
      },
      "required": [
        "objects"
      ],
      "type": "object"
    },
    "signature": "'(*, objects: Annotated[tuple[riverhog_storage_adapter_protocol.protocol.ObjectLocator, ...], MinLen(min_length=1)]) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ReadPreparationRequest",
  "unit": "export"
}
```

</details>
