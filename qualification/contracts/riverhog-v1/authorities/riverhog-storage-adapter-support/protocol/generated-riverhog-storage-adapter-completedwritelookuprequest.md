# generated:riverhog-storage-adapter: CompletedWriteLookupRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-comple-0fb020c5d9:7492d0919a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-95c307eef1aa"></a>
- <a id="s-03703c29e8e5"></a>`title`: CompletedWriteLookupRequest
- <a id="s-37f9a223b987"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d6b613d62580"></a>`expected_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-558690d0bf39"></a>`expected_content_type` | yes | type="string"; minLength=1; maxLength=255 |  |
| <a id="s-8c0b60131f43"></a>`expected_placement` | yes | type="string"; enum=["archive","immediate"] |  |
| <a id="s-ceec1bb3a766"></a>`object_path` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-dc8a372d2af1"></a>`required_identity_assertions` | yes | type="object"; additional keys=`additionalProperties`, `maxProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` | Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions. |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field expected_content_type](#s-558690d0bf39) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field object_path](#s-ceec1bb3a766) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field required_identity_assertions](#s-dc8a372d2af1) | `encoded-size · bytes · contract_max` | maximum=16384; reason="bounded-object-identity-assertion-envelope"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field required_identity_assertions](#s-dc8a372d2af1) | `cardinality · entries · contract_max` | maximum=64; reason="bounded-object-identity-assertion-envelope" |

## Governing policies

- <a id="pa-a98d09e4923d"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-2abce7befb42"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471a9) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/CompletedWriteLookupRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3f6289ea7cdb8562a130c3d3df28e4217ae0490a38d84f2f2158dafb98e18689 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "expected_bytes": {
      "minimum": 1,
      "title": "Expected Bytes",
      "type": "integer"
    },
    "expected_content_type": {
      "maxLength": 255,
      "minLength": 1,
      "title": "Expected Content Type",
      "type": "string"
    },
    "expected_placement": {
      "enum": [
        "archive",
        "immediate"
      ],
      "title": "Expected Placement",
      "type": "string"
    },
    "object_path": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Object Path",
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
    "expected_content_type",
    "required_identity_assertions",
    "expected_placement"
  ],
  "title": "CompletedWriteLookupRequest",
  "type": "object"
}
```
