# generated:riverhog-storage-adapter: StorageAdapterConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-storag-b0b8ffd825:838a72f9a0 -->

Stable positive evidence returned after the complete check set passes.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-d1efcb8390"></a>

- <a id="s-8e95ad152b"></a>`type`: `"object"`
- <a id="s-ae81d78185"></a>`additionalProperties`: `false`
- <a id="s-8b747e633b"></a>`description`: `"Stable positive evidence returned after the complete check set passes."`
- <a id="s-6a46179014"></a>`required`: `["descriptor","checks"]`
- <a id="s-4c4e89854c"></a>`title`: `"StorageAdapterConformanceResult"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ee5757dee3"></a>`checks` | yes | type="array"; items=(type="string"); title="Checks" |  |
| <a id="s-c25d196dae"></a>`coverage` | no | type="string"; const="complete"; default="complete"; title="Coverage" |  |
| <a id="s-6eaad858d9"></a>`descriptor` | yes | [AdapterDescriptor](#s-941580622e) |  |
| <a id="s-6166d1ce52"></a>`format` | no | type="string"; const="riverhog-storage-adapter-conformance-result/v1"; default="riverhog-storage-adapter-conformance-result/v1"; title="Format" |  |
| <a id="s-7a5a35d6e2"></a>`protocol` | no | type="string"; const="riverhog-storage-adapter/v1"; default="riverhog-storage-adapter/v1"; title="Protocol" |  |
| <a id="s-0d4e848375"></a>`status` | no | type="string"; const="conformant"; default="conformant"; title="Status" |  |

### Definitions

- [AdapterDescriptor](#s-941580622e)

### <a id="s-941580622e"></a>definition `AdapterDescriptor`

- <a id="s-1bbfa1af1b"></a>`type`: `"object"`
- <a id="s-e8c19c8ddb"></a>`additionalProperties`: `false`
- <a id="s-0e8429e873"></a>`required`: `["implementation_id","implementation_version","read_mode","minimum_nonfinal_segment_bytes"]`
- <a id="s-a417bb9ca6"></a>`title`: `"AdapterDescriptor"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f1b79988eb"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Implementation Id" |  |
| <a id="s-92ca0aae95"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1; title="Implementation Version" |  |
| <a id="s-48168b80c7"></a>`maximum_segment_bytes` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null; title="Maximum Segment Bytes" |  |
| <a id="s-60e6132e30"></a>`maximum_segment_count` | no | anyOf=[(type="integer"; minimum=1); (type="null")]; default=null; title="Maximum Segment Count" |  |
| <a id="s-65f932beae"></a>`minimum_nonfinal_segment_bytes` | yes | type="integer"; minimum=1; title="Minimum Nonfinal Segment Bytes" |  |
| <a id="s-f2cd45f88a"></a>`protocol` | no | type="string"; const="riverhog-storage-adapter/v1"; default="riverhog-storage-adapter/v1"; title="Protocol" |  |
| <a id="s-849366dd4b"></a>`read_mode` | yes | type="string"; enum=["immediate","restore_required"]; title="Read Mode" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-storage-adapter-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field checks](#s-ee5757dee3) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=120; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition AdapterDescriptor · field implementation_version](#s-92ca0aae95) | `length · characters · contract_max` | shared above |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-e93a62855e"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-66fa794c5f"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-c9029fbd1a"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/StorageAdapterConformanceResult`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af805afd4d0dffb329ba08dde971222ad008eab2aec7afc4abc0303dd1956caf -->

```json
{
  "$defs": {
    "AdapterDescriptor": {
      "additionalProperties": false,
      "properties": {
        "implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Implementation Id",
          "type": "string"
        },
        "implementation_version": {
          "maxLength": 120,
          "minLength": 1,
          "title": "Implementation Version",
          "type": "string"
        },
        "maximum_segment_bytes": {
          "anyOf": [
            {
              "minimum": 1,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Maximum Segment Bytes"
        },
        "maximum_segment_count": {
          "anyOf": [
            {
              "minimum": 1,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Maximum Segment Count"
        },
        "minimum_nonfinal_segment_bytes": {
          "minimum": 1,
          "title": "Minimum Nonfinal Segment Bytes",
          "type": "integer"
        },
        "protocol": {
          "const": "riverhog-storage-adapter/v1",
          "default": "riverhog-storage-adapter/v1",
          "title": "Protocol",
          "type": "string"
        },
        "read_mode": {
          "enum": [
            "immediate",
            "restore_required"
          ],
          "title": "Read Mode",
          "type": "string"
        }
      },
      "required": [
        "implementation_id",
        "implementation_version",
        "read_mode",
        "minimum_nonfinal_segment_bytes"
      ],
      "title": "AdapterDescriptor",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "description": "Stable positive evidence returned after the complete check set passes.",
  "properties": {
    "checks": {
      "items": {
        "type": "string"
      },
      "title": "Checks",
      "type": "array"
    },
    "coverage": {
      "const": "complete",
      "default": "complete",
      "title": "Coverage",
      "type": "string"
    },
    "descriptor": {
      "$ref": "#/$defs/AdapterDescriptor"
    },
    "format": {
      "const": "riverhog-storage-adapter-conformance-result/v1",
      "default": "riverhog-storage-adapter-conformance-result/v1",
      "title": "Format",
      "type": "string"
    },
    "protocol": {
      "const": "riverhog-storage-adapter/v1",
      "default": "riverhog-storage-adapter/v1",
      "title": "Protocol",
      "type": "string"
    },
    "status": {
      "const": "conformant",
      "default": "conformant",
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "descriptor",
    "checks"
  ],
  "title": "StorageAdapterConformanceResult",
  "type": "object"
}
```

</details>
