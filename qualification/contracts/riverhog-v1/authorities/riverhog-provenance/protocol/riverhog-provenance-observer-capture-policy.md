# Riverhog provenance observer capture policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance:riverhog-provenance-observer-capture-policy:521de4b76c -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1provenance~1observers~1schemas~1observation-policy.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json` — `packages/riverhog-provenance/src/riverhog_provenance/schemas/observation-policy.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json
- `title`: Riverhog provenance observer capture policy
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `attempt_noatime` | yes | boolean |  |
| `capture_acl` | yes | boolean |  |
| `capture_file_flags` | yes | boolean |  |
| `capture_native_stat` | yes | boolean |  |
| `capture_sparse_map` | yes | boolean |  |
| `capture_special_features` | yes | boolean |  |
| `capture_system_acl` | yes | boolean |  |
| `capture_xattrs` | yes | boolean |  |
| `hash_chunk_bytes` | yes | integer |  |
| `include_access_time` | yes | boolean |  |
| `include_effective_principal` | yes | boolean |  |
| `include_hostname` | yes | boolean |  |
| `inline_native_value_bytes` | yes | integer |  |
| `large_value_disposition` | yes | object (1 fields) |  |
| `maximum_native_streams` | yes | integer |  |
| `maximum_native_value_bytes` | yes | integer |  |
| `maximum_sparse_extents` | yes | integer |  |
| `native_stream_chunk_bytes` | yes | integer |  |
| `resolve_principals` | yes | boolean |  |
| `resource_fork_chunk_bytes` | yes | integer |  |
| `second_content_hash` | yes | boolean |  |
| `strict_consistency` | yes | boolean |  |
| `verify_path_binding` | yes | boolean |  |
| `windows_allow_shared_delete` | yes | boolean |  |
| `windows_allow_shared_write` | yes | boolean |  |
| `windows_capture_object_id` | yes | boolean |  |
| `windows_capture_usn` | yes | boolean |  |
| `windows_follow_non_name_surrogate_reparse_points` | yes | boolean |  |

## Complete owned contract

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
