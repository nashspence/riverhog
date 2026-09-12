# generated:riverhog-storage-adapter: StorageAdapterConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-storag-b0b8ffd825:e78895edd0 -->

Stable positive evidence returned after the complete check set passes.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-d1efcb83902a"></a>
- <a id="s-4c4e89854cc9"></a>`title`: StorageAdapterConformanceResult
- <a id="s-8b747e633b97"></a>`description`: Stable positive evidence returned after the complete check set passes.
- <a id="s-8e95ad152b14"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ee5757dee321"></a>`checks` | yes | type="array"; items=(type="string") |  |
| <a id="s-c25d196daeba"></a>`coverage` | no | type="string"; const="complete" |  |
| <a id="s-6eaad858d9cd"></a>`descriptor` | yes | #/$defs/AdapterDescriptor |  |
| <a id="s-6166d1ce52c6"></a>`format` | no | type="string"; const="riverhog-storage-adapter-conformance-result/v1" |  |
| <a id="s-7a5a35d6e2f3"></a>`protocol` | no | type="string"; const="riverhog-storage-adapter/v1" |  |
| <a id="s-0d4e848375c6"></a>`status` | no | type="string"; const="conformant" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-941580622ec6"></a>`AdapterDescriptor` | type="object"; fields=`implementation_id`, `implementation_version`, `maximum_segment_bytes`, `maximum_segment_count`, `minimum_nonfinal_segment_bytes`, `protocol`, `read_mode`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-storage-adapter-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field checks](#s-ee5757dee321) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=120; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-92ca0aae9564"></a>definition AdapterDescriptor · field implementation_version | `length · characters · contract_max` | shared above |

## Governing policies

- <a id="pa-5946f0b285e9"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-422cf9150f44"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-c643ed932f9c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471a9) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/StorageAdapterConformanceResult`

### Exact owned JSON

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
