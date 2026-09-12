# generated:riverhog-storage-adapter: WriteStartRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writestartrequest:78b9e07616 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-storage-adapter-support` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteStartRequest`

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
| length | characters | `contract_max` | maximum=4096, minimum=1, reason=schema-maximum |
| encoded-size | bytes | `contract_max` | maximum=16384, reason=bounded-object-identity-assertion-envelope |
| cardinality | entries | `contract_max` | maximum=64, reason=bounded-object-identity-assertion-envelope |

## Contract summary

- `title`: WriteStartRequest
- `description`: Exact authority for one idempotently established nonterminal write.  Repeating the same canonical request against the same configured adapter while the write remains nonterminal returns the same continuation session. Operational credentials used to realize that session remain adapter-private.
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `content_type` | yes | string |  |
| `expected_bytes` | yes | integer |  |
| `object_path` | yes | string |  |
| `placement` | yes | string |  |
| `required_identity_assertions` | yes | object | Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions. |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4394746d6639230cabebf5d57a6e369f5e1401fe144a77641c7eb0aff144d2fa -->

```json
{
  "additionalProperties": false,
  "description": "Exact authority for one idempotently established nonterminal write.\n\nRepeating the same canonical request against the same configured adapter while the\nwrite remains nonterminal returns the same continuation session. Operational\ncredentials used to realize that session remain adapter-private.",
  "properties": {
    "content_type": {
      "maxLength": 255,
      "minLength": 1,
      "title": "Content Type",
      "type": "string"
    },
    "expected_bytes": {
      "minimum": 1,
      "title": "Expected Bytes",
      "type": "integer"
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
    }
  },
  "required": [
    "object_path",
    "expected_bytes",
    "content_type",
    "required_identity_assertions",
    "placement"
  ],
  "title": "WriteStartRequest",
  "type": "object"
}
```
