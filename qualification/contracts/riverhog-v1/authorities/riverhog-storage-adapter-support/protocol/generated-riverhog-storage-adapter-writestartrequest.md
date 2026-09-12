# generated:riverhog-storage-adapter: WriteStartRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writestartrequest:78b9e07616 -->

Exact authority for one idempotently established nonterminal write.

Repeating the same canonical request against the same configured adapter while the
write remains nonterminal returns the same continuation session. Operational
credentials used to realize that session remain adapter-private.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-2b36e52b84d2"></a>
- <a id="s-2e642e279f9a"></a>`title`: WriteStartRequest
- <a id="s-5b9e227aa357"></a>`description`: Exact authority for one idempotently established nonterminal write.  Repeating the same canonical request against the same configured adapter while the write remains nonterminal returns the same continuation session. Operational credentials used to realize that session remain adapter-private.
- <a id="s-5de2718e1e6f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f983ec79476a"></a>`content_type` | yes | type="string"; minLength=1; maxLength=255 |  |
| <a id="s-ba391b396484"></a>`expected_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-59aac5a432bc"></a>`object_path` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-23f1edee7b26"></a>`placement` | yes | type="string"; enum=["archive","immediate"] |  |
| <a id="s-3f20a9ba4dfb"></a>`required_identity_assertions` | yes | type="object"; additional keys=`additionalProperties`, `maxProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` | Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions. |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field content_type](#s-f983ec79476a) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field object_path](#s-59aac5a432bc) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field required_identity_assertions](#s-3f20a9ba4dfb) | `encoded-size · bytes · contract_max` | maximum=16384; reason="bounded-object-identity-assertion-envelope"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field required_identity_assertions](#s-3f20a9ba4dfb) | `cardinality · entries · contract_max` | maximum=64; reason="bounded-object-identity-assertion-envelope" |

## Governing policies

- <a id="pa-136ff9d4a323"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-f7d328dbfc29"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471a9) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteStartRequest`

### Exact owned JSON

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
