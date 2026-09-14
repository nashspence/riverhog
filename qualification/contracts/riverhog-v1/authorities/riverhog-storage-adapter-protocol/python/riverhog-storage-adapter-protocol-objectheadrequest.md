# riverhog_storage_adapter_protocol.ObjectHeadRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectheadrequest:c864ad29b1 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-057720001d"></a>`signature`: `"\"(*, object: riverhog_storage_adapter_protocol.protocol.ObjectLocator, expected_placement: Literal['archive', 'immediate']) -> None\""`

#### Validated model schema

<a id="s-70d5e9eea3"></a>
- <a id="s-e0ac15e6f5"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1798c06a34"></a>`expected_placement` | yes | type="string"; enum=["archive","immediate"] |  |
| <a id="s-01cf494aa8"></a>`object` | yes | #/$defs/ObjectLocator |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-6b8a5bc6a1"></a>`ObjectLocator` | type="object"; fields=`object_path`, `revision`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-c5e98fc1b4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectHeadRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 620f7a9e3aeb13b966599512ed96907754da14748a771d250a87a7a7b21dd595 -->

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
        "expected_placement": {
          "enum": [
            "archive",
            "immediate"
          ],
          "type": "string"
        },
        "object": {
          "$ref": "#/$defs/ObjectLocator"
        }
      },
      "required": [
        "object",
        "expected_placement"
      ],
      "type": "object"
    },
    "signature": "\"(*, object: riverhog_storage_adapter_protocol.protocol.ObjectLocator, expected_placement: Literal['archive', 'immediate']) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ObjectHeadRequest",
  "unit": "export"
}
```
