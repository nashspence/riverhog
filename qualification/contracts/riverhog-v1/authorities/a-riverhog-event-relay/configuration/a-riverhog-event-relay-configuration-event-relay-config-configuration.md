# a-riverhog-event-relay:configuration:event-relay-config configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:a-riverhog-event-relay:a-riverhog-event-relay-configuration-even-348a3e86fe:7c53b077c7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-event-relay](../index.md) |
| Interface | [Configuration Documents](index.md) |

## External contract

<a id="s-f40c23f549"></a>

- <a id="s-8286f8de9e"></a>`type`: `"object"`
- <a id="s-6b9677ef17"></a>`additionalProperties`: `false`
- <a id="s-73b2756a68"></a>`required`: `["state_path","sources"]`
- <a id="s-12e3daa577"></a>`title`: `"EventRelayConfig"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-05f05b1004"></a>`batch_size` | no | type="integer"; minimum=1; maximum=100; default=100; title="Batch Size" |  |
| <a id="s-87ad350d34"></a>`poll_interval_seconds` | no | type="number"; default=5; exclusiveMinimum=0; title="Poll Interval Seconds" |  |
| <a id="s-de1f2dff32"></a>`request_timeout_seconds` | no | type="number"; default=10; exclusiveMinimum=0; title="Request Timeout Seconds" |  |
| <a id="s-f81c005b3d"></a>`sources` | yes | type="array"; items=([SourceConfig](#s-163e667ada)); minItems=1; title="Sources" |  |
| <a id="s-16a8929c57"></a>`state_path` | yes | type="string"; format="path"; title="State Path" |  |
| <a id="s-af7a846ff4"></a>`version` | no | type="integer"; default=1; title="Version" |  |

### Definitions

- [SourceConfig](#s-163e667ada)

### <a id="s-163e667ada"></a>definition `SourceConfig`

- <a id="s-cecef785c6"></a>`type`: `"object"`
- <a id="s-d167e79bb6"></a>`additionalProperties`: `false`
- <a id="s-bedc976a1e"></a>`required`: `["name","events_url","token_env","webhook_url_env"]`
- <a id="s-579fda1d5b"></a>`title`: `"SourceConfig"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f6c70f9ac1"></a>`events_url` | yes | type="string"; minLength=1; title="Events Url" |  |
| <a id="s-49f0f9b143"></a>`name` | yes | type="string"; minLength=1; pattern="^[A-Za-z0-9._-]+$"; title="Name" |  |
| <a id="s-b0a160f43a"></a>`token_env` | yes | type="string"; minLength=1; title="Token Env" |  |
| <a id="s-5d2372dc90"></a>`webhook_url_env` | yes | type="string"; minLength=1; title="Webhook Url Env" |  |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-riverhog-event-relay:configuration:event-relay-config"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sources](#s-f81c005b3d) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field batch_size](#s-05f05b1004) | `value · schema-value · contract_max` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-a448d79072"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-81bb2f229a"></a>[extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)
- <a id="pa-ca7fafedd1"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration:a-riverhog-event-relay:configuration:event-relay-config](../../../evidence/sources/authorities.md#src-7bdba96f2b) — [some-implementations/riverhog/applications/a-riverhog-event-relay/src/a\_riverhog\_event\_relay/relay.py::EventRelayConfig](../../../../../../some-implementations/riverhog/applications/a-riverhog-event-relay/src/a_riverhog_event_relay/relay.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/configuration_documents/a-riverhog-event-relay:configuration:event-relay-config`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b76bd416664590dffc5dd040ab003e7fe37e6bfed90feb5fc2e90b07b21f9f12 -->

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
  "title": "EventRelayConfig",
  "type": "object"
}
```

</details>
