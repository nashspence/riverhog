# riverhog_storage_adapter_protocol.ReadStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-readstatus:57c1fe427a -->

Exact externally visible contract owned by this contract element.

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

- <a id="s-b80a47717f"></a>`type`: `"object"`
- <a id="s-a261d21f6d"></a>`additionalProperties`: `false`
- <a id="s-5840f1fa90"></a>`required`: `["objects","readiness"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0cf2e8a02f"></a>`objects` | yes | type="array"; items=([ObjectLocator](#s-f5cb8a00e4)); minItems=1 |  |
| <a id="s-126151cd38"></a>`readiness` | yes | discriminator={"mapping":{"expired":"#/$defs/ReadExpired","ready":"#/$defs/ReadReady","requested":"#/$defs/ReadRequested"},"propertyName":"state"}; oneOf=[([ReadRequested](#s-8fc985c6ca)); ([ReadReady](#s-eb22b97e33)); ([ReadExpired](#s-89cf76aaa6))] |  |

##### Definitions

- [ObjectLocator](#s-f5cb8a00e4)
- [ReadExpired](#s-89cf76aaa6)
- [ReadReady](#s-eb22b97e33)
- [ReadRequested](#s-8fc985c6ca)

##### <a id="s-f5cb8a00e4"></a>definition `ObjectLocator`

- <a id="s-7420492f0f"></a>`type`: `"object"`
- <a id="s-94dda0550c"></a>`additionalProperties`: `false`
- <a id="s-6168882348"></a>`required`: `["object_path"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6e85852882"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-0d1aff8a0c"></a>`revision` | no | anyOf=[(type="string"; maxLength=2000; minLength=1); (type="null")]; default=null |  |

##### <a id="s-89cf76aaa6"></a>definition `ReadExpired`

- <a id="s-574311949c"></a>`type`: `"object"`
- <a id="s-0a33f7e734"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5de65d8b06"></a>`state` | no | type="string"; const="expired"; default="expired" |  |

##### <a id="s-eb22b97e33"></a>definition `ReadReady`

- <a id="s-2ab005545d"></a>`type`: `"object"`
- <a id="s-adcde2cbca"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-93905fe13a"></a>`available_until` | no | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; default=null |  |
| <a id="s-f15fd3367c"></a>`state` | no | type="string"; const="ready"; default="ready" |  |

##### <a id="s-8fc985c6ca"></a>definition `ReadRequested`

- <a id="s-9e52905660"></a>`type`: `"object"`
- <a id="s-c2c66efcda"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-998c2d9989"></a>`estimated_ready_at` | no | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; default=null |  |
| <a id="s-ef4920cf1d"></a>`state` | no | type="string"; const="requested"; default="requested" |  |

## Maintained corroboration

### Related interface records

- [canonical_objects](riverhog-storage-adapter-protocol-readstatus-canonical-objects.md)

## Governing policies

- <a id="pa-00785524c2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ReadStatus`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 51f3355f1ab394c5b983082d6521a894e5f0edd4d2a7da337c24447cea673cb3 -->

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
                  "maxLength": 30,
                  "minLength": 30,
                  "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
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
                  "maxLength": 30,
                  "minLength": 30,
                  "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
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

</details>
