# mango-fish configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:mango-fish:mango-fish-configuration:cf7172ea24 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [mango-fish](../index.md) |
| Interface | [configuration](index.md) |
| Family | [documents](index.md#f-551542af63f5) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-b7221b191411"></a>
- <a id="s-ba7b2362843f"></a>`title`: MangoFishConfig
- <a id="s-959e7a3119db"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2ba3abdea0cc"></a>`batch_size` | no | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-196293757f2b"></a>`poll_interval_seconds` | no | type="number"; additional keys=`exclusiveMinimum` |  |
| <a id="s-2cc8058e6e43"></a>`request_timeout_seconds` | no | type="number"; additional keys=`exclusiveMinimum` |  |
| <a id="s-32bdb9c709fd"></a>`sources` | yes | type="array"; minItems=1; items=(#/$defs/SourceConfig) |  |
| <a id="s-5a007c0c3187"></a>`state_path` | yes | type="string"; format="path" |  |
| <a id="s-24bb31026471"></a>`version` | no | type="integer" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-5ed11d56c7a9"></a>`SourceConfig` | type="object"; fields=`events_url`, `name`, `token_env`, `webhook_url_env`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e519)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"mango-fish"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sources](#s-32bdb9c709fd) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field batch_size](#s-2ba3abdea0cc) | `value · schema-value · contract_max` | shared above |

## Governing policies

- <a id="pa-e6c99f2f8713"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)
- <a id="pa-880ba9cf555c"></a>[extent-rule/configuration-composition/v1](../../../policies/index.md#p-dcd344e8e519)
- <a id="pa-7a57b2e33548"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration:mango-fish](../../../evidence/sources.md#src-168bf78fe3f0) — `reference/riverhog/applications/mango-fish/src/mango_fish/relay.py::MangoFishConfig`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

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
