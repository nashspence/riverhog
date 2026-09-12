# generated:riverhog-storage-adapter: ReadStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-readstatus:0669d2c5b9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- `title`: ReadStatus
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `objects` | yes | type="array"; minItems=1; items=(#/$defs/ObjectLocator) |  |
| `readiness` | yes | oneOf=#/$defs/ReadRequested \| #/$defs/ReadReady \| #/$defs/ReadExpired; additional keys=`discriminator` |  |

### Definitions

| Definition | Shape |
|---|---|
| `ObjectLocator` | type="object"; fields=`object_path`, `revision`; additional keys=`additionalProperties`, `required` |
| `ReadExpired` | type="object"; fields=`state`; additional keys=`additionalProperties` |
| `ReadReady` | type="object"; fields=`available_until`, `state`; additional keys=`additionalProperties` |
| `ReadRequested` | type="object"; fields=`estimated_ready_at`, `state`; additional keys=`additionalProperties` |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| cardinality | items | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |

## Governing policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

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
