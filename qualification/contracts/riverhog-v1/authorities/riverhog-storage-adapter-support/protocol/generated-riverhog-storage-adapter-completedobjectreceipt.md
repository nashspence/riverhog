# generated:riverhog-storage-adapter: CompletedObjectReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-comple-18fc1686ec:5487e99746 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 7 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/CompletedObjectReceipt`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:generated:riverhog-storage-adapter` — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=4000, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=2000, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=255, minimum=1, reason=schema-maximum |
| encoded-size | bytes | `contract_max` | maximum=16384, reason=bounded-object-identity-assertion-envelope |
| cardinality | entries | `contract_max` | maximum=64, reason=bounded-object-identity-assertion-envelope |

## Contract summary

- `title`: CompletedObjectReceipt
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `completed_at` | yes | string |  |
| `entity_token` | no | object (3 fields) |  |
| `object_path` | yes | string |  |
| `revision` | no | object (3 fields) |  |
| `stored_bytes` | yes | integer |  |
| `verified_content_type` | yes | string |  |
| `verified_identity_assertions` | yes | object | Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions. |
| `verified_placement` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 209031565e0ef889aecef270878eea3fc57d0b15a42c37d7610d1dbf208def6a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "completed_at": {
      "maxLength": 100,
      "minLength": 1,
      "title": "Completed At",
      "type": "string"
    },
    "entity_token": {
      "anyOf": [
        {
          "maxLength": 4000,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Entity Token"
    },
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
    },
    "stored_bytes": {
      "minimum": 1,
      "title": "Stored Bytes",
      "type": "integer"
    },
    "verified_content_type": {
      "maxLength": 255,
      "minLength": 1,
      "title": "Verified Content Type",
      "type": "string"
    },
    "verified_identity_assertions": {
      "additionalProperties": {
        "type": "string"
      },
      "description": "Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions.",
      "maxProperties": 64,
      "title": "Verified Identity Assertions",
      "type": "object",
      "x-riverhog-encoded-bytes-max": 16384,
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-object-identity-assertion-envelope"
      }
    },
    "verified_placement": {
      "enum": [
        "archive",
        "immediate"
      ],
      "title": "Verified Placement",
      "type": "string"
    }
  },
  "required": [
    "object_path",
    "stored_bytes",
    "verified_content_type",
    "verified_identity_assertions",
    "verified_placement",
    "completed_at"
  ],
  "title": "CompletedObjectReceipt",
  "type": "object"
}
```
