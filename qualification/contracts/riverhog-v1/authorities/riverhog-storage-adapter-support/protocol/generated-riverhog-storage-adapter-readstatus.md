# generated:riverhog-storage-adapter: ReadStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-readstatus:0669d2c5b9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-d7e97ded86d3"></a>
- <a id="s-503de59ef936"></a>`title`: ReadStatus
- <a id="s-b939c496b739"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4344c1608a17"></a>`objects` | yes | type="array"; minItems=1; items=(#/$defs/ObjectLocator) |  |
| <a id="s-f6a789a1fb0b"></a>`readiness` | yes | oneOf=#/$defs/ReadRequested \| #/$defs/ReadReady \| #/$defs/ReadExpired; additional keys=`discriminator` |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-d7a7425eaaf2"></a>`ObjectLocator` | type="object"; fields=`object_path`, `revision`; additional keys=`additionalProperties`, `required` |
| <a id="s-285138754e89"></a>`ReadExpired` | type="object"; fields=`state`; additional keys=`additionalProperties` |
| <a id="s-e03e1a2323e4"></a>`ReadReady` | type="object"; fields=`available_until`, `state`; additional keys=`additionalProperties` |
| <a id="s-c23f02270ea0"></a>`ReadRequested` | type="object"; fields=`estimated_ready_at`, `state`; additional keys=`additionalProperties` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-storage-adapter-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field objects](#s-4344c1608a17) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-b6fb00700158"></a>definition ReadReady · field available_until · anyOf alternative 1 | `length · characters · contract_max` | shared above |
| <a id="s-ab68818a64fd"></a>definition ReadRequested · field estimated_ready_at · anyOf alternative 1 | `length · characters · contract_max` | shared above |

## Governing policies

- <a id="pa-74fabeecd837"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-20be29f932ae"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-d78954e71e51"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471a9) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/ReadStatus`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c2e15c25cf8ffc82c41b134eb06692ae3c3138226cfdc381c64e277774f403a6 -->

```json
{
  "$defs": {
    "ObjectLocator": {
      "additionalProperties": false,
      "properties": {
        "object_path": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Object Path",
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
          "default": null,
          "title": "Revision"
        }
      },
      "required": [
        "object_path"
      ],
      "title": "ObjectLocator",
      "type": "object"
    },
    "ReadExpired": {
      "additionalProperties": false,
      "properties": {
        "state": {
          "const": "expired",
          "default": "expired",
          "title": "State",
          "type": "string"
        }
      },
      "title": "ReadExpired",
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
          "default": null,
          "title": "Available Until"
        },
        "state": {
          "const": "ready",
          "default": "ready",
          "title": "State",
          "type": "string"
        }
      },
      "title": "ReadReady",
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
          "default": null,
          "title": "Estimated Ready At"
        },
        "state": {
          "const": "requested",
          "default": "requested",
          "title": "State",
          "type": "string"
        }
      },
      "title": "ReadRequested",
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
      "title": "Objects",
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
      ],
      "title": "Readiness"
    }
  },
  "required": [
    "objects",
    "readiness"
  ],
  "title": "ReadStatus",
  "type": "object"
}
```
