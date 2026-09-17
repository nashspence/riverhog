# generated:riverhog-storage-adapter: ReadStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-readstatus:28a540460c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-d7e97ded86"></a>

- <a id="s-b939c496b7"></a>`type`: `"object"`
- <a id="s-594e9cd362"></a>`additionalProperties`: `false`
- <a id="s-2a59149353"></a>`required`: `["objects","readiness"]`
- <a id="s-503de59ef9"></a>`title`: `"ReadStatus"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4344c1608a"></a>`objects` | yes | type="array"; items=([ObjectLocator](#s-d7a7425eaa)); minItems=1; title="Objects" |  |
| <a id="s-f6a789a1fb"></a>`readiness` | yes | discriminator={"mapping":{"expired":"#/$defs/ReadExpired","ready":"#/$defs/ReadReady","requested":"#/$defs/ReadRequested"},"propertyName":"state"}; oneOf=[([ReadRequested](#s-c23f02270e)); ([ReadReady](#s-e03e1a2323)); ([ReadExpired](#s-285138754e))]; title="Readiness" |  |

### Definitions

- [ObjectLocator](#s-d7a7425eaa)
- [ReadExpired](#s-285138754e)
- [ReadReady](#s-e03e1a2323)
- [ReadRequested](#s-c23f02270e)

### <a id="s-d7a7425eaa"></a>definition `ObjectLocator`

- <a id="s-cd344d52b7"></a>`type`: `"object"`
- <a id="s-e98d368795"></a>`additionalProperties`: `false`
- <a id="s-e64b6387cf"></a>`required`: `["object_path"]`
- <a id="s-e5e0127bf1"></a>`title`: `"ObjectLocator"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c1fb151a13"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1; title="Object Path" |  |
| <a id="s-10c072a33f"></a>`revision` | no | anyOf=[(type="string"; maxLength=2000; minLength=1); (type="null")]; default=null; title="Revision" |  |

### <a id="s-285138754e"></a>definition `ReadExpired`

- <a id="s-bd756d95f3"></a>`type`: `"object"`
- <a id="s-30003bac3b"></a>`additionalProperties`: `false`
- <a id="s-3e45ee3692"></a>`title`: `"ReadExpired"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-36323568e0"></a>`state` | no | type="string"; const="expired"; default="expired"; title="State" |  |

### <a id="s-e03e1a2323"></a>definition `ReadReady`

- <a id="s-f90716a20c"></a>`type`: `"object"`
- <a id="s-bef2594833"></a>`additionalProperties`: `false`
- <a id="s-0fa95a4ebc"></a>`title`: `"ReadReady"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8f1c09c270"></a>`available_until` | no | anyOf=[(type="string"; maxLength=100; minLength=1); (type="null")]; default=null; title="Available Until" |  |
| <a id="s-6593f5851e"></a>`state` | no | type="string"; const="ready"; default="ready"; title="State" |  |

### <a id="s-c23f02270e"></a>definition `ReadRequested`

- <a id="s-ab9c6a7ac2"></a>`type`: `"object"`
- <a id="s-6847afb26c"></a>`additionalProperties`: `false`
- <a id="s-8c5e8ad085"></a>`title`: `"ReadRequested"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ed34680d49"></a>`estimated_ready_at` | no | anyOf=[(type="string"; maxLength=100; minLength=1); (type="null")]; default=null; title="Estimated Ready At" |  |
| <a id="s-a235938066"></a>`state` | no | type="string"; const="requested"; default="requested"; title="State" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-storage-adapter-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field objects](#s-4344c1608a) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=100; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-b6fb007001"></a>[definition ReadReady · field available_until · string value](#s-8f1c09c270) | `length · characters · contract_max` | shared above |
| <a id="s-ab68818a64"></a>[definition ReadRequested · field estimated_ready_at · string value](#s-ed34680d49) | `length · characters · contract_max` | shared above |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-a1620a9fa2"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-10929e59e5"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-4a15f28372"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/ReadStatus`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
