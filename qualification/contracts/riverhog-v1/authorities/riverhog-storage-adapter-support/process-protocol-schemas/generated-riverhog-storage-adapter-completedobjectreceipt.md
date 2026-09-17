# generated:riverhog-storage-adapter: CompletedObjectReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-comple-18fc1686ec:fb853b63f0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-17b1471cc8"></a>

- <a id="s-1ef964e063"></a>`type`: `"object"`
- <a id="s-3b71cc4256"></a>`additionalProperties`: `false`
- <a id="s-4c1410aa9f"></a>`required`: `["object_path","stored_bytes","verified_content_type","verified_identity_assertions","verified_placement","completed_at"]`
- <a id="s-9c9e0ba05f"></a>`title`: `"CompletedObjectReceipt"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e321434eb8"></a>`completed_at` | yes | type="string"; maxLength=100; minLength=1; title="Completed At" |  |
| <a id="s-f9fbc589a8"></a>`entity_token` | no | anyOf=[(type="string"; maxLength=4000; minLength=1); (type="null")]; default=null; title="Entity Token" |  |
| <a id="s-627e7fc470"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1; title="Object Path" |  |
| <a id="s-a35df82ae3"></a>`revision` | no | anyOf=[(type="string"; maxLength=2000; minLength=1); (type="null")]; default=null; title="Revision" |  |
| <a id="s-4ade09495c"></a>`stored_bytes` | yes | type="integer"; minimum=1; title="Stored Bytes" |  |
| <a id="s-e975843e16"></a>`verified_content_type` | yes | type="string"; maxLength=255; minLength=1; title="Verified Content Type" |  |
| <a id="s-23cb851317"></a>`verified_identity_assertions` | yes | type="object"; additionalProperties=(type="string"); maxProperties=64; title="Verified Identity Assertions"; x-riverhog-encoded-bytes-max=16384; x-riverhog-extent={"policy":"contract_max","reason":"bounded-object-identity-assertion-envelope"} | Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions. |
| <a id="s-786e57377f"></a>`verified_placement` | yes | type="string"; enum=["archive","immediate"]; title="Verified Placement" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field completed_at](#s-e321434eb8) | `length · characters · contract_max` | maximum=100; minimum=1; reason="schema-maximum" |
| <a id="s-5a60785583"></a>[field entity_token · string value](#s-f9fbc589a8) | `length · characters · contract_max` | maximum=4000; minimum=1; reason="schema-maximum" |
| [field object_path](#s-627e7fc470) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| <a id="s-ed019f2a33"></a>[field revision · string value](#s-a35df82ae3) | `length · characters · contract_max` | maximum=2000; minimum=1; reason="schema-maximum" |
| [field verified_content_type](#s-e975843e16) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field verified_identity_assertions](#s-23cb851317) | `encoded-size · bytes · contract_max` | maximum=16384; reason="bounded-object-identity-assertion-envelope"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field verified_identity_assertions](#s-23cb851317) | `cardinality · entries · contract_max` | maximum=64; reason="bounded-object-identity-assertion-envelope" |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-369ccb7ede"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-353bcdbc18"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/CompletedObjectReceipt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
