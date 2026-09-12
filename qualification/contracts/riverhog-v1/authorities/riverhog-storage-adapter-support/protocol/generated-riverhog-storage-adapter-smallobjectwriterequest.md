# generated:riverhog-storage-adapter: SmallObjectWriteRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-smallo-12ddfe9106:e0bc7707e7 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 6 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/SmallObjectWriteRequest`

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
| length | characters | `contract_max` | maximum=255, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| encoded-size | bytes | `contract_max` | maximum=16384, reason=bounded-object-identity-assertion-envelope |
| cardinality | entries | `contract_max` | maximum=64, reason=bounded-object-identity-assertion-envelope |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: SmallObjectWriteRequest
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `content_type` | yes | string |  |
| `expected_current_stored_sha256` | no | object (3 fields) |  |
| `mode` | yes | string |  |
| `object_path` | yes | string |  |
| `placement` | yes | string |  |
| `required_identity_assertions` | yes | object | Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions. |
| `stored_bytes` | yes | integer |  |
| `stored_sha256` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 80a724477b2bedfe5fa69bfd96c571cd25a684c741a0e8d5a715c15bfc25c270 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "content_type": {
      "maxLength": 255,
      "minLength": 1,
      "title": "Content Type",
      "type": "string"
    },
    "expected_current_stored_sha256": {
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
      "title": "Expected Current Stored Sha256"
    },
    "mode": {
      "enum": [
        "create_only",
        "replace_current"
      ],
      "title": "Mode",
      "type": "string"
    },
    "object_path": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Object Path",
      "type": "string"
    },
    "placement": {
      "enum": [
        "archive",
        "immediate"
      ],
      "title": "Placement",
      "type": "string"
    },
    "required_identity_assertions": {
      "additionalProperties": {
        "type": "string"
      },
      "description": "Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions.",
      "maxProperties": 64,
      "title": "Required Identity Assertions",
      "type": "object",
      "x-riverhog-encoded-bytes-max": 16384,
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-object-identity-assertion-envelope"
      }
    },
    "stored_bytes": {
      "minimum": 0,
      "title": "Stored Bytes",
      "type": "integer"
    },
    "stored_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Stored Sha256",
      "type": "string"
    }
  },
  "required": [
    "object_path",
    "content_type",
    "required_identity_assertions",
    "placement",
    "mode",
    "stored_bytes",
    "stored_sha256"
  ],
  "title": "SmallObjectWriteRequest",
  "type": "object"
}
```
