# generated:riverhog-storage-adapter: ObjectMetadataReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-object-729ea22b16:b7aa723a76 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 8 |

## External contract

<a id="s-ce25cafb079e"></a>
- <a id="s-7317b0829ca2"></a>`title`: ObjectMetadataReceipt
- <a id="s-f0cc3d401309"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2ab02b518530"></a>`completed_at` | yes | type="string"; minLength=1; maxLength=100 |  |
| <a id="s-8e9adb40afd9"></a>`content_type` | no | anyOf=type="string"; minLength=1; maxLength=255 \| type="null" |  |
| <a id="s-72ecba381aa3"></a>`entity_token` | no | anyOf=type="string"; minLength=1; maxLength=4000 \| type="null" |  |
| <a id="s-2e4dd05b513b"></a>`object_path` | yes | type="string"; minLength=1; maxLength=4096 |  |
| <a id="s-b8e148ba238e"></a>`observed_identity_assertions` | yes | type="object"; additional keys=`additionalProperties`, `maxProperties`, `x-riverhog-encoded-bytes-max`, `x-riverhog-extent` | Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions. |
| <a id="s-8d6db6545521"></a>`revision` | no | anyOf=type="string"; minLength=1; maxLength=2000 \| type="null" |  |
| <a id="s-03ecfd198fce"></a>`stored_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-fd6d184c14ac"></a>`stored_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-9740c0377ac7"></a>`verified_placement` | yes | type="string"; enum=["archive","immediate"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field completed_at](#s-2ab02b518530) | `length · characters · contract_max` | maximum=100; minimum=1; reason="schema-maximum" |
| <a id="s-3345ebca9bc7"></a>field content_type · anyOf alternative 1 | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| <a id="s-9b5572a12189"></a>field entity_token · anyOf alternative 1 | `length · characters · contract_max` | maximum=4000; minimum=1; reason="schema-maximum" |
| [field object_path](#s-2e4dd05b513b) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field observed_identity_assertions](#s-b8e148ba238e) | `encoded-size · bytes · contract_max` | maximum=16384; reason="bounded-object-identity-assertion-envelope"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field observed_identity_assertions](#s-b8e148ba238e) | `cardinality · entries · contract_max` | maximum=64; reason="bounded-object-identity-assertion-envelope" |
| <a id="s-8e32a74ccce2"></a>field revision · anyOf alternative 1 | `length · characters · contract_max` | maximum=2000; minimum=1; reason="schema-maximum" |
| <a id="s-7c197ec48e9b"></a>field stored_sha256 · anyOf alternative 1 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

- <a id="pa-ab741ae16fb5"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-1f28b313e912"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471a9) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/ObjectMetadataReceipt`

### Exact owned JSON

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
