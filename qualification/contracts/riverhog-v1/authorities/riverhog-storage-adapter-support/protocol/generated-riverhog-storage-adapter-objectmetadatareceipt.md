# generated:riverhog-storage-adapter: ObjectMetadataReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-object-729ea22b16:b7aa723a76 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 8 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/ObjectMetadataReceipt`

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
| length | characters | `contract_max` | maximum=255, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=4000, minimum=1, reason=schema-maximum |
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| encoded-size | bytes | `contract_max` | maximum=16384, reason=bounded-object-identity-assertion-envelope |
| cardinality | entries | `contract_max` | maximum=64, reason=bounded-object-identity-assertion-envelope |
| length | characters | `contract_max` | maximum=2000, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: ObjectMetadataReceipt
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `completed_at` | yes | string |  |
| `content_type` | no | object (3 fields) |  |
| `entity_token` | no | object (3 fields) |  |
| `object_path` | yes | string |  |
| `observed_identity_assertions` | yes | object | Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions. |
| `revision` | no | object (3 fields) |  |
| `stored_bytes` | yes | integer |  |
| `stored_sha256` | no | object (3 fields) |  |
| `verified_placement` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 10fe081f0530594bffeb9d17545b2652dd8db57725461bb29fe06922e61a7c67 -->

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
    "content_type": {
      "anyOf": [
        {
          "maxLength": 255,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Content Type"
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
    "observed_identity_assertions": {
      "additionalProperties": {
        "type": "string"
      },
      "description": "Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions.",
      "maxProperties": 64,
      "title": "Observed Identity Assertions",
      "type": "object",
      "x-riverhog-encoded-bytes-max": 16384,
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-object-identity-assertion-envelope"
      }
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
      "minimum": 0,
      "title": "Stored Bytes",
      "type": "integer"
    },
    "stored_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Stored Sha256"
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
    "observed_identity_assertions",
    "verified_placement",
    "completed_at"
  ],
  "title": "ObjectMetadataReceipt",
  "type": "object"
}
```
