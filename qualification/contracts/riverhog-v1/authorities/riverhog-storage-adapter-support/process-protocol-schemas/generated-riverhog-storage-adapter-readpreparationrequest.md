# generated:riverhog-storage-adapter: ReadPreparationRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-readpr-ab55ff7a63:9d840e68a7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-4173c7b1a9"></a>

- <a id="s-60a7ccf92b"></a>`type`: `"object"`
- <a id="s-2d1a3b3204"></a>`additionalProperties`: `false`
- <a id="s-d9a68f6935"></a>`required`: `["objects"]`
- <a id="s-3bbfe82e59"></a>`title`: `"ReadPreparationRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-60e69d3f7d"></a>`objects` | yes | type="array"; items=([ObjectLocator](#s-1d34366070)); minItems=1; title="Objects" |  |

### Definitions

- [ObjectLocator](#s-1d34366070)

### <a id="s-1d34366070"></a>definition `ObjectLocator`

- <a id="s-e4b66e5931"></a>`type`: `"object"`
- <a id="s-9d0a2bad5a"></a>`additionalProperties`: `false`
- <a id="s-4a01ab5806"></a>`required`: `["object_path"]`
- <a id="s-51fb2052b1"></a>`title`: `"ObjectLocator"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f856f6fc3d"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1; title="Object Path" |  |
| <a id="s-0ef58f764d"></a>`revision` | no | anyOf=[(type="string"; maxLength=2000; minLength=1); (type="null")]; default=null; title="Revision" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-storage-adapter-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field objects](#s-60e69d3f7d) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-63d114e281"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-aba137fd76"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/ReadPreparationRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a1096dde3495131448c778944da805d24cbfabda7630aa9d927684235cb42635 -->

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
    }
  },
  "required": [
    "objects"
  ],
  "title": "ReadPreparationRequest",
  "type": "object"
}
```

</details>
