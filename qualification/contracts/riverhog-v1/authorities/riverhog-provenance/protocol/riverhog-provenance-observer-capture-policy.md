# Riverhog provenance observer capture policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance:riverhog-provenance-observer-capture-policy:521de4b76c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

- `$id`: https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json
- `title`: Riverhog provenance observer capture policy
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `attempt_noatime` | yes | type="boolean" |  |
| `capture_acl` | yes | type="boolean" |  |
| `capture_file_flags` | yes | type="boolean" |  |
| `capture_native_stat` | yes | type="boolean" |  |
| `capture_sparse_map` | yes | type="boolean" |  |
| `capture_special_features` | yes | type="boolean" |  |
| `capture_system_acl` | yes | type="boolean" |  |
| `capture_xattrs` | yes | type="boolean" |  |
| `hash_chunk_bytes` | yes | type="integer"; minimum=1 |  |
| `include_access_time` | yes | type="boolean" |  |
| `include_effective_principal` | yes | type="boolean" |  |
| `include_hostname` | yes | type="boolean" |  |
| `inline_native_value_bytes` | yes | type="integer"; minimum=1 |  |
| `large_value_disposition` | yes | enum=["digest_only","not_retained","fail"] |  |
| `maximum_native_streams` | yes | type="integer"; minimum=1 |  |
| `maximum_native_value_bytes` | yes | type="integer"; minimum=1 |  |
| `maximum_sparse_extents` | yes | type="integer"; minimum=1 |  |
| `native_stream_chunk_bytes` | yes | type="integer"; minimum=1 |  |
| `resolve_principals` | yes | type="boolean" |  |
| `resource_fork_chunk_bytes` | yes | type="integer"; minimum=1 |  |
| `second_content_hash` | yes | type="boolean" |  |
| `strict_consistency` | yes | type="boolean" |  |
| `verify_path_binding` | yes | type="boolean" |  |
| `windows_allow_shared_delete` | yes | type="boolean" |  |
| `windows_allow_shared_write` | yes | type="boolean" |  |
| `windows_capture_object_id` | yes | type="boolean" |  |
| `windows_capture_usn` | yes | type="boolean" |  |
| `windows_follow_non_name_surrogate_reparse_points` | yes | type="boolean" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Governing policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json` — `packages/riverhog-provenance/src/riverhog_provenance/schemas/observation-policy.schema.json`

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
