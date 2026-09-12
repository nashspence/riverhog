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

## Contract

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
