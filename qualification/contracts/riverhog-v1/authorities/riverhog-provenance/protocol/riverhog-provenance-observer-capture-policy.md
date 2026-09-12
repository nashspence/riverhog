# Riverhog provenance observer capture policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance:riverhog-provenance-observer-capture-policy:521de4b76c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-6adfbad66e) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-73d5738dc1"></a>
- <a id="s-8d35beac79"></a>`$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json
- <a id="s-bc53812e0c"></a>`title`: Riverhog provenance observer capture policy
- <a id="s-e6dc5b0285"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d0f0d33145"></a>`attempt_noatime` | yes | type="boolean" |  |
| <a id="s-560507324a"></a>`capture_acl` | yes | type="boolean" |  |
| <a id="s-9dec0d0a4f"></a>`capture_file_flags` | yes | type="boolean" |  |
| <a id="s-9ac327bdd7"></a>`capture_native_stat` | yes | type="boolean" |  |
| <a id="s-ef91e4c95f"></a>`capture_sparse_map` | yes | type="boolean" |  |
| <a id="s-552dc49dbf"></a>`capture_special_features` | yes | type="boolean" |  |
| <a id="s-b4f4c413b2"></a>`capture_system_acl` | yes | type="boolean" |  |
| <a id="s-9a9b5576d5"></a>`capture_xattrs` | yes | type="boolean" |  |
| <a id="s-b3b9a4c840"></a>`hash_chunk_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-fec7a8f8c0"></a>`include_access_time` | yes | type="boolean" |  |
| <a id="s-806e32274e"></a>`include_effective_principal` | yes | type="boolean" |  |
| <a id="s-760158ca96"></a>`include_hostname` | yes | type="boolean" |  |
| <a id="s-619fa64027"></a>`inline_native_value_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-a33fca4fba"></a>`large_value_disposition` | yes | enum=["digest_only","not_retained","fail"] |  |
| <a id="s-374b14223f"></a>`maximum_native_streams` | yes | type="integer"; minimum=1 |  |
| <a id="s-cd52cd1e87"></a>`maximum_native_value_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-6e8c99c8c8"></a>`maximum_sparse_extents` | yes | type="integer"; minimum=1 |  |
| <a id="s-29e17fa094"></a>`native_stream_chunk_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-0f7319aa84"></a>`resolve_principals` | yes | type="boolean" |  |
| <a id="s-c2f9cc12d6"></a>`resource_fork_chunk_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-bbe2c25e43"></a>`second_content_hash` | yes | type="boolean" |  |
| <a id="s-7aa27f8526"></a>`strict_consistency` | yes | type="boolean" |  |
| <a id="s-4377328b9b"></a>`verify_path_binding` | yes | type="boolean" |  |
| <a id="s-b60bf9ce82"></a>`windows_allow_shared_delete` | yes | type="boolean" |  |
| <a id="s-b263f59967"></a>`windows_allow_shared_write` | yes | type="boolean" |  |
| <a id="s-d27c4cae23"></a>`windows_capture_object_id` | yes | type="boolean" |  |
| <a id="s-40e4c81b39"></a>`windows_capture_usn` | yes | type="boolean" |  |
| <a id="s-161f614662"></a>`windows_follow_non_name_surrogate_reparse_points` | yes | type="boolean" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field hash_chunk_bytes](#s-b3b9a4c840) | `value · schema-value · operational_policy` | shared above |
| [field inline_native_value_bytes](#s-619fa64027) | `value · schema-value · operational_policy` | shared above |
| [field maximum_native_value_bytes](#s-cd52cd1e87) | `value · schema-value · operational_policy` | shared above |
| [field native_stream_chunk_bytes](#s-29e17fa094) | `value · schema-value · operational_policy` | shared above |
| [field resource_fork_chunk_bytes](#s-c2f9cc12d6) | `value · schema-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-d7b99aae75"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-a933b6ea34"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json](../../../evidence/sources.md#src-9581f745b7) — `packages/riverhog-provenance/src/riverhog_provenance/schemas/observation-policy.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1observation-policy.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d9894d2447f874171e535da375d90680f6de65a3e28b91d564f94f633e5d8ac9 -->

```json
{
  "$id": "https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "attempt_noatime": {
      "type": "boolean"
    },
    "capture_acl": {
      "type": "boolean"
    },
    "capture_file_flags": {
      "type": "boolean"
    },
    "capture_native_stat": {
      "type": "boolean"
    },
    "capture_sparse_map": {
      "type": "boolean"
    },
    "capture_special_features": {
      "type": "boolean"
    },
    "capture_system_acl": {
      "type": "boolean"
    },
    "capture_xattrs": {
      "type": "boolean"
    },
    "hash_chunk_bytes": {
      "minimum": 1,
      "type": "integer"
    },
    "include_access_time": {
      "type": "boolean"
    },
    "include_effective_principal": {
      "type": "boolean"
    },
    "include_hostname": {
      "type": "boolean"
    },
    "inline_native_value_bytes": {
      "minimum": 1,
      "type": "integer"
    },
    "large_value_disposition": {
      "enum": [
        "digest_only",
        "not_retained",
        "fail"
      ]
    },
    "maximum_native_streams": {
      "minimum": 1,
      "type": "integer"
    },
    "maximum_native_value_bytes": {
      "minimum": 1,
      "type": "integer"
    },
    "maximum_sparse_extents": {
      "minimum": 1,
      "type": "integer"
    },
    "native_stream_chunk_bytes": {
      "minimum": 1,
      "type": "integer"
    },
    "resolve_principals": {
      "type": "boolean"
    },
    "resource_fork_chunk_bytes": {
      "minimum": 1,
      "type": "integer"
    },
    "second_content_hash": {
      "type": "boolean"
    },
    "strict_consistency": {
      "type": "boolean"
    },
    "verify_path_binding": {
      "type": "boolean"
    },
    "windows_allow_shared_delete": {
      "type": "boolean"
    },
    "windows_allow_shared_write": {
      "type": "boolean"
    },
    "windows_capture_object_id": {
      "type": "boolean"
    },
    "windows_capture_usn": {
      "type": "boolean"
    },
    "windows_follow_non_name_surrogate_reparse_points": {
      "type": "boolean"
    }
  },
  "required": [
    "strict_consistency",
    "attempt_noatime",
    "verify_path_binding",
    "second_content_hash",
    "hash_chunk_bytes",
    "inline_native_value_bytes",
    "maximum_native_value_bytes",
    "large_value_disposition",
    "capture_xattrs",
    "capture_acl",
    "capture_file_flags",
    "capture_sparse_map",
    "capture_special_features",
    "capture_native_stat",
    "resolve_principals",
    "include_access_time",
    "include_hostname",
    "include_effective_principal",
    "maximum_sparse_extents",
    "resource_fork_chunk_bytes",
    "native_stream_chunk_bytes",
    "maximum_native_streams",
    "capture_system_acl",
    "windows_allow_shared_write",
    "windows_allow_shared_delete",
    "windows_follow_non_name_surrogate_reparse_points",
    "windows_capture_usn",
    "windows_capture_object_id"
  ],
  "title": "Riverhog provenance observer capture policy",
  "type": "object"
}
```
