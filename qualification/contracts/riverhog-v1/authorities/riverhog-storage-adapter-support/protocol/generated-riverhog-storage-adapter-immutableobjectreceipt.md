# generated:riverhog-storage-adapter: ImmutableObjectReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-immuta-8a6f47c3bf:4179aaf59d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 8 |

## External contract

<a id="s-3fb47aaf09"></a>
- <a id="s-d161b1c6dd"></a>`title`: ImmutableObjectReceipt
- <a id="s-a408c9d96c"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a100c52725"></a>`completed_at` | yes | type="string"; minLength=1; maxLength=100 |  |
| <a id="s-42c0358383"></a>`entity_token` | no | anyOf=type="string"; minLength=1; maxLength=4000 \| type="null" |  |
| <a id="s-6ac6921944"></a>`object_path` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-c6dc53bb9f"></a>`revision` | no | anyOf=type="string"; minLength=1; maxLength=2000 \| type="null" |  |
| <a id="s-4a063b6979"></a>`stored_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-2806c9d5c6"></a>`stored_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-eb1ed81ecd"></a>`verified_content_type` | yes | type="string"; minLength=1; maxLength=255 |  |
| <a id="s-f13441d8ea"></a>`verified_identity_assertions` | yes | type="object"; additional keys=`additionalProperties`, `maxProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` | Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions. |
| <a id="s-107d1345ef"></a>`verified_placement` | yes | type="string"; enum=["archive","immediate"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field completed_at](#s-a100c52725) | `length · characters · contract_max` | maximum=100; minimum=1; reason="schema-maximum" |
| <a id="s-280e9e3e20"></a>[field entity_token · string value](#s-42c0358383) | `length · characters · contract_max` | maximum=4000; minimum=1; reason="schema-maximum" |
| [field object_path](#s-6ac6921944) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| <a id="s-5a5ab3b2ce"></a>[field revision · string value](#s-c6dc53bb9f) | `length · characters · contract_max` | maximum=2000; minimum=1; reason="schema-maximum" |
| [field stored_sha256](#s-2806c9d5c6) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field verified_content_type](#s-eb1ed81ecd) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field verified_identity_assertions](#s-f13441d8ea) | `encoded-size · bytes · contract_max` | maximum=16384; reason="bounded-object-identity-assertion-envelope"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field verified_identity_assertions](#s-f13441d8ea) | `cardinality · entries · contract_max` | maximum=64; reason="bounded-object-identity-assertion-envelope" |

## Governing policies

- <a id="pa-e8a595718a"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-581ded9be9"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/ImmutableObjectReceipt`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4246d342ca4bea1f0e76e6b31ee8d998513e8d604a1b2ef888c96c7a0776196d -->

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
      "minimum": 0,
      "title": "Stored Bytes",
      "type": "integer"
    },
    "stored_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Stored Sha256",
      "type": "string"
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
    "stored_sha256",
    "verified_content_type",
    "verified_identity_assertions",
    "verified_placement",
    "completed_at"
  ],
  "title": "ImmutableObjectReceipt",
  "type": "object"
}
```
