# riverhog_storage_adapter_protocol.ReadStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-readstatus:57c1fe427a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-119d3c48aa"></a>
- <a id="s-049185a14a"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-de74da1e28"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-f331165b3f"></a>`name`: `ReadStatus`
- <a id="s-f701940259"></a>`unit`: `export`

### Declared structure

- <a id="s-3b0060e15d"></a>`kind`: `"class"`
- <a id="s-6a9cd63ba0"></a>`signature`: `"'(*, objects: Annotated[tuple[riverhog_storage_adapter_protocol.protocol.ObjectLocator, ...], MinLen(min_length=1)], readiness: riverhog_storage_adapter_protocol.protocol.ReadRequested \| riverhog_storage_adapter_protocol.protocol.ReadReady \| riverhog_storage_adapter_protocol.protocol.ReadExpired) -> None'"`

#### Validated model schema

<a id="s-d40a7ea33e"></a>
- <a id="s-b80a47717f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0cf2e8a02f"></a>`objects` | yes | type="array"; minItems=1; items=(#/$defs/ObjectLocator) |  |
| <a id="s-126151cd38"></a>`readiness` | yes | oneOf=#/$defs/ReadRequested \| #/$defs/ReadReady \| #/$defs/ReadExpired; additional keys=`discriminator` |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-f5cb8a00e4"></a>`ObjectLocator` | type="object"; fields=`object_path`, `revision`; additional keys=`additionalProperties`, `required` |
| <a id="s-89cf76aaa6"></a>`ReadExpired` | type="object"; fields=`state`; additional keys=`additionalProperties` |
| <a id="s-eb22b97e33"></a>`ReadReady` | type="object"; fields=`available_until`, `state`; additional keys=`additionalProperties` |
| <a id="s-8fc985c6ca"></a>`ReadRequested` | type="object"; fields=`estimated_ready_at`, `state`; additional keys=`additionalProperties` |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ReadStatus.canonical_objects](riverhog-storage-adapter-protocol-readstatus-canonical-objects.md)

## Governing policies

- <a id="pa-00785524c2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ReadStatus`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4801558f74f25e288cb663cab652684ce44fd72b4119a6e25e1c7be07bf3eacd -->

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
        },
        "ReadExpired": {
          "additionalProperties": false,
          "properties": {
            "state": {
              "const": "expired",
              "default": "expired",
              "type": "string"
            }
          },
          "type": "object"
        },
        "ReadReady": {
          "additionalProperties": false,
          "properties": {
            "available_until": {
              "anyOf": [
                {
                  "maxLength": 100,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "state": {
              "const": "ready",
              "default": "ready",
              "type": "string"
            }
          },
          "type": "object"
        },
        "ReadRequested": {
          "additionalProperties": false,
          "properties": {
            "estimated_ready_at": {
              "anyOf": [
                {
                  "maxLength": 100,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "state": {
              "const": "requested",
              "default": "requested",
              "type": "string"
            }
          },
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
        },
        "readiness": {
          "discriminator": {
            "mapping": {
              "expired": "#/$defs/ReadExpired",
              "ready": "#/$defs/ReadReady",
              "requested": "#/$defs/ReadRequested"
            },
            "propertyName": "state"
          },
          "oneOf": [
            {
              "$ref": "#/$defs/ReadRequested"
            },
            {
              "$ref": "#/$defs/ReadReady"
            },
            {
              "$ref": "#/$defs/ReadExpired"
            }
          ]
        }
      },
      "required": [
        "objects",
        "readiness"
      ],
      "type": "object"
    },
    "signature": "'(*, objects: Annotated[tuple[riverhog_storage_adapter_protocol.protocol.ObjectLocator, ...], MinLen(min_length=1)], readiness: riverhog_storage_adapter_protocol.protocol.ReadRequested | riverhog_storage_adapter_protocol.protocol.ReadReady | riverhog_storage_adapter_protocol.protocol.ReadExpired) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ReadStatus",
  "unit": "export"
}
```
