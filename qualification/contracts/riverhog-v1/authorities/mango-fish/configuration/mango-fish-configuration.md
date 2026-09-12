# mango-fish configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:mango-fish:mango-fish-configuration:cf7172ea24 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `mango-fish` |
| Interface | `configuration` |
| Family | `documents` |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

- `title`: MangoFishConfig
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `batch_size` | no | type="integer"; minimum=1; maximum=100 |  |
| `poll_interval_seconds` | no | type="number"; additional keys=`exclusiveMinimum` |  |
| `request_timeout_seconds` | no | type="number"; additional keys=`exclusiveMinimum` |  |
| `sources` | yes | type="array"; minItems=1; items=(#/$defs/SourceConfig) |  |
| `state_path` | yes | type="string"; format="path" |  |
| `version` | no | type="integer" |  |

### Definitions

| Definition | Shape |
|---|---|
| `SourceConfig` | type="object"; fields=`events_url`, `name`, `token_env`, `webhook_url_env`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| value | schema-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |
| cardinality | items | `operational_policy` | maximum=None, reason=validated-deployment-composition |

## Governing policies

- `compatibility/configuration/v1`
- `extent-rule/configuration-composition/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make unit`
- `make compose-smoke`

### Executable sources

- `configuration:mango-fish` — `reference/riverhog/applications/mango-fish/src/mango_fish/relay.py::MangoFishConfig`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_documents/mango-fish`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f7bc3073b0a14e26758eede785d9217ba35409ab00e28e8facc3f79947222dc1 -->

```json
{
  "$defs": {
    "SourceConfig": {
      "additionalProperties": false,
      "properties": {
        "events_url": {
          "minLength": 1,
          "title": "Events Url",
          "type": "string"
        },
        "name": {
          "minLength": 1,
          "pattern": "^[A-Za-z0-9._-]+$",
          "title": "Name",
          "type": "string"
        },
        "token_env": {
          "minLength": 1,
          "title": "Token Env",
          "type": "string"
        },
        "webhook_url_env": {
          "minLength": 1,
          "title": "Webhook Url Env",
          "type": "string"
        }
      },
      "required": [
        "name",
        "events_url",
        "token_env",
        "webhook_url_env"
      ],
      "title": "SourceConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "batch_size": {
      "default": 100,
      "maximum": 100,
      "minimum": 1,
      "title": "Batch Size",
      "type": "integer"
    },
    "poll_interval_seconds": {
      "default": 5,
      "exclusiveMinimum": 0,
      "title": "Poll Interval Seconds",
      "type": "number"
    },
    "request_timeout_seconds": {
      "default": 10,
      "exclusiveMinimum": 0,
      "title": "Request Timeout Seconds",
      "type": "number"
    },
    "sources": {
      "items": {
        "$ref": "#/$defs/SourceConfig"
      },
      "minItems": 1,
      "title": "Sources",
      "type": "array"
    },
    "state_path": {
      "format": "path",
      "title": "State Path",
      "type": "string"
    },
    "version": {
      "default": 1,
      "title": "Version",
      "type": "integer"
    }
  },
  "required": [
    "state_path",
    "sources"
  ],
  "title": "MangoFishConfig",
  "type": "object"
}
```
