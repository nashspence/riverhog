# generated:riverhog-storage-adapter: StorageAdapterError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-storag-7d05cfb36b:e90b0c6399 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-81e0279481"></a>

- <a id="s-d54a2fa63d"></a>`type`: `"object"`
- <a id="s-d5ba6c7b0a"></a>`additionalProperties`: `false`
- <a id="s-dfeb4fa1e9"></a>`required`: `["error"]`
- <a id="s-15a4c3c3d3"></a>`title`: `"StorageAdapterError"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-896bc94219"></a>`error` | yes | [StorageAdapterErrorBody](#s-16d9d4653c) |  |

### Definitions

- [StorageAdapterErrorBody](#s-16d9d4653c)

### <a id="s-16d9d4653c"></a>definition `StorageAdapterErrorBody`

- <a id="s-0460b5335e"></a>`type`: `"object"`
- <a id="s-fa5c4b1a13"></a>`additionalProperties`: `false`
- <a id="s-d92551f155"></a>`required`: `["code","message"]`
- <a id="s-b7181d9ef7"></a>`title`: `"StorageAdapterErrorBody"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f0c7f497d1"></a>`code` | yes | type="string"; enum=["unauthorized","invalid_request","not_found","method_not_allowed","length_required","request_too_large","insufficient_storage","identity_conflict","traversal_invalidated","invalid_path","invalid_range","read_not_ready","read_expired","integrity_failure","provider_unavailable","internal_failure"]; title="Code" |  |
| <a id="s-396a1bf2d8"></a>`message` | yes | type="string"; maxLength=2000; minLength=1; title="Message" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=2000; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition StorageAdapterErrorBody · field message](#s-396a1bf2d8) | `length · characters · contract_max` | shared above |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-167f8c6bea"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-91cb7bec39"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/StorageAdapterError`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 19b906afcdf8d94d77f5aef65b4facf0cb084d382ef94d440ae8a5cd58e7aa7f -->

```json
{
  "$defs": {
    "StorageAdapterErrorBody": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "enum": [
            "unauthorized",
            "invalid_request",
            "not_found",
            "method_not_allowed",
            "length_required",
            "request_too_large",
            "insufficient_storage",
            "identity_conflict",
            "traversal_invalidated",
            "invalid_path",
            "invalid_range",
            "read_not_ready",
            "read_expired",
            "integrity_failure",
            "provider_unavailable",
            "internal_failure"
          ],
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 2000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        }
      },
      "required": [
        "code",
        "message"
      ],
      "title": "StorageAdapterErrorBody",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "error": {
      "$ref": "#/$defs/StorageAdapterErrorBody"
    }
  },
  "required": [
    "error"
  ],
  "title": "StorageAdapterError",
  "type": "object"
}
```

</details>
