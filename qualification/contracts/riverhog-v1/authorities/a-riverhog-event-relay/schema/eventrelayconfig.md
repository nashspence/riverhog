# EventRelayConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:a-riverhog-event-relay:eventrelayconfig:f500245366 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-event-relay](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-7258488fbf"></a>

- <a id="s-b799a3f852"></a>`type`: `"object"`
- <a id="s-631821d7c7"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/config/a-riverhog-event-relay.schema.json"`
- <a id="s-f4ca392a2d"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-9a6400b2d5"></a>`additionalProperties`: `false`
- <a id="s-27e19a1d2c"></a>`required`: `["state_path","sources"]`
- <a id="s-fe3cd83c9a"></a>`title`: `"EventRelayConfig"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-929bd1020e"></a>`batch_size` | no | type="integer"; minimum=1; maximum=100; default=100; title="Batch Size" |  |
| <a id="s-747f367874"></a>`poll_interval_seconds` | no | type="number"; default=5; exclusiveMinimum=0; title="Poll Interval Seconds" |  |
| <a id="s-a76e745a56"></a>`request_timeout_seconds` | no | type="number"; default=10; exclusiveMinimum=0; title="Request Timeout Seconds" |  |
| <a id="s-8e9608c94f"></a>`sources` | yes | type="array"; items=([SourceConfig](#s-78d177da79)); minItems=1; title="Sources" |  |
| <a id="s-089424af88"></a>`state_path` | yes | type="string"; format="path"; title="State Path" |  |
| <a id="s-2dec0c2015"></a>`version` | no | type="integer"; default=1; title="Version" |  |

### Definitions

- [SourceConfig](#s-78d177da79)

### <a id="s-78d177da79"></a>definition `SourceConfig`

- <a id="s-c16caf2cbb"></a>`type`: `"object"`
- <a id="s-78294ebb50"></a>`additionalProperties`: `false`
- <a id="s-c4781db6fe"></a>`required`: `["name","events_url","token_file","webhook_url_file"]`
- <a id="s-b72d4a507c"></a>`title`: `"SourceConfig"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-57a5f2cbbd"></a>`events_url` | yes | type="string"; minLength=1; title="Events Url" |  |
| <a id="s-9bcda8cab3"></a>`name` | yes | type="string"; minLength=1; pattern="^[A-Za-z0-9._-]+$"; title="Name" |  |
| <a id="s-a54e72e7eb"></a>`token_file` | yes | type="string"; format="path"; title="Token File" |  |
| <a id="s-6ca18d7097"></a>`webhook_url_file` | yes | type="string"; format="path"; title="Webhook Url File" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/config/a-riverhog-event-relay.schema.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sources](#s-8e9608c94f) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field batch_size](#s-929bd1020e) | `value · schema-value · contract_max` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-e91d7cb818"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-c5104619af"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-d28f686ef1"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/config/a-riverhog-event-relay.schema.json](../../../evidence/sources/authorities.md#src-6c5f509b0a) — [some-implementations/riverhog/applications/a-riverhog-event-relay/src/a\_riverhog\_event\_relay/config.schema.json](../../../../../../some-implementations/riverhog/applications/a-riverhog-event-relay/src/a_riverhog_event_relay/config.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1config~1a-riverhog-event-relay.schema.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd781d51982d9bda6a9632dc44b2f9e2cc78bfa7513d8b332b6691386785ff98 -->

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
        "token_file": {
          "format": "path",
          "title": "Token File",
          "type": "string"
        },
        "webhook_url_file": {
          "format": "path",
          "title": "Webhook Url File",
          "type": "string"
        }
      },
      "required": [
        "name",
        "events_url",
        "token_file",
        "webhook_url_file"
      ],
      "title": "SourceConfig",
      "type": "object"
    }
  },
  "$id": "https://nashspence.github.io/riverhog/v1/config/a-riverhog-event-relay.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
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
  "title": "EventRelayConfig",
  "type": "object"
}
```

</details>
