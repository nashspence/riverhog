# mango-fish:configuration:mango-fish-config configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:mango-fish:mango-fish-configuration-mango-fish-confi-23883d4fd8:833944d1e0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [mango-fish](../index.md) |
| Interface | [configuration](index.md) |
| Family | [documents](index.md#f-551542af63) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-19de51639f"></a>
- <a id="s-e67bca452e"></a>`title`: MangoFishConfig
- <a id="s-9ae9fa9a9f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e58849def3"></a>`batch_size` | no | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-4983396f84"></a>`poll_interval_seconds` | no | type="number"; additional keys=`exclusiveMinimum` |  |
| <a id="s-65906d0517"></a>`request_timeout_seconds` | no | type="number"; additional keys=`exclusiveMinimum` |  |
| <a id="s-bbb230564e"></a>`sources` | yes | type="array"; minItems=1; items=(#/$defs/SourceConfig) |  |
| <a id="s-1bf9c39c6c"></a>`state_path` | yes | type="string"; format="path" |  |
| <a id="s-4b9b4c32a2"></a>`version` | no | type="integer" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-ccfb15558e"></a>`SourceConfig` | type="object"; fields=`events_url`, `name`, `token_env`, `webhook_url_env`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"mango-fish:configuration:mango-fish-config"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sources](#s-bbb230564e) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field batch_size](#s-e58849def3) | `value · schema-value · contract_max` | shared above |

## Governing policies

- <a id="pa-019b1e9ee4"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)
- <a id="pa-915f82c123"></a>[extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e5)
- <a id="pa-61ff6dd44d"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration:mango-fish:configuration:mango-fish-config](../../../evidence/sources.md#src-fead015e98) — `reference/riverhog/applications/mango-fish/src/mango_fish/relay.py::MangoFishConfig`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_documents/mango-fish:configuration:mango-fish-config`

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
