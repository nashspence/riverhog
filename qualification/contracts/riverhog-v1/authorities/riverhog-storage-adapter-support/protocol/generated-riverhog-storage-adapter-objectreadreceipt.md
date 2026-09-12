# generated:riverhog-storage-adapter: ObjectReadReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-objectreadreceipt:e833f18a4c -->

Adapter-observed identity and range for one single-pass read.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-af23daf6d7b3"></a>
- <a id="s-e97b07454c76"></a>`title`: ObjectReadReceipt
- <a id="s-0eb9c3d72479"></a>`description`: Adapter-observed identity and range for one single-pass read.
- <a id="s-fd118908e70c"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-036b19f39b5f"></a>`object` | yes | #/$defs/ObjectLocator |  |
| <a id="s-66b82b7e4fc5"></a>`offset` | yes | type="integer"; minimum=0 |  |
| <a id="s-cbc2c869b9cb"></a>`read_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-214b4e2d3019"></a>`total_bytes` | yes | type="integer"; minimum=0 |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-43c741ab8f7b"></a>`ObjectLocator` | type="object"; fields=`object_path`, `revision`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-storage-adapter-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field offset](#s-66b82b7e4fc5) | `value · schema-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-19dcdcd752ff"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-5a3fc7d1f6c2"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471a9) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/ObjectReadReceipt`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c00ab742b44d1b708bac1d252dd1dd6fb4882bdec441eb330f63746d635820ce -->

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
  "description": "Adapter-observed identity and range for one single-pass read.",
  "properties": {
    "object": {
      "$ref": "#/$defs/ObjectLocator"
    },
    "offset": {
      "minimum": 0,
      "title": "Offset",
      "type": "integer"
    },
    "read_bytes": {
      "minimum": 0,
      "title": "Read Bytes",
      "type": "integer"
    },
    "total_bytes": {
      "minimum": 0,
      "title": "Total Bytes",
      "type": "integer"
    }
  },
  "required": [
    "object",
    "total_bytes",
    "offset",
    "read_bytes"
  ],
  "title": "ObjectReadReceipt",
  "type": "object"
}
```
