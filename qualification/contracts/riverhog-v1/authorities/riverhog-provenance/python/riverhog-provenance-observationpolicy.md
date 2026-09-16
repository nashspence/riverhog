# riverhog_provenance.ObservationPolicy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-observationpolicy:1fa8366cb0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1781cb19dd"></a>
- <a id="s-56e9fc1c25"></a>`distribution`: `riverhog-provenance`
- <a id="s-4ab1201058"></a>`module`: `riverhog_provenance`
- <a id="s-4a0c4dc701"></a>`name`: `ObservationPolicy`
- <a id="s-71dd70e809"></a>`unit`: `export`

### Declared structure

- <a id="s-59e0bb053d"></a>`kind`: `"class"`
- <a id="s-b0d6ac3eb4"></a>`signature`: `"\"(strict_consistency: 'bool' = True, attempt_noatime: 'bool' = True, verify_path_binding: 'bool' = True, second_content_hash: 'bool' = False, hash_chunk_bytes: 'int' = 8388608, inline_native_value_bytes: 'int' = 1048576, maximum_native_value_bytes: 'int' = 268435456, large_value_disposition: 'LargeValueDisposition' = <LargeValueDisposition.DIGEST_ONLY: 'digest_only'>, capture_xattrs: 'bool' = True, capture_acl: 'bool' = True, capture_file_flags: 'bool' = True, capture_sparse_map: 'bool' = True, capture_special_features: 'bool' = True, capture_native_stat: 'bool' = True, resolve_principals: 'bool' = True, include_access_time: 'bool' = True, include_hostname: 'bool' = True, include_effective_principal: 'bool' = True, maximum_sparse_extents: 'int' = 100000, resource_fork_chunk_bytes: 'int' = 8388608, native_stream_chunk_bytes: 'int' = 8388608, maximum_native_streams: 'int' = 10000, capture_system_acl: 'bool' = False, windows_allow_shared_write: 'bool' = False, windows_allow_shared_delete: 'bool' = False, windows_follow_non_name_surrogate_reparse_points: 'bool' = True, windows_capture_usn: 'bool' = True, windows_capture_object_id: 'bool' = True) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-677339099c"></a>`strict_consistency` | `'bool'` | `True` |
| <a id="s-36af6d821a"></a>`attempt_noatime` | `'bool'` | `True` |
| <a id="s-63fee5a05c"></a>`verify_path_binding` | `'bool'` | `True` |
| <a id="s-47619e18ef"></a>`second_content_hash` | `'bool'` | `False` |
| <a id="s-f4531f351a"></a>`hash_chunk_bytes` | `'int'` | `8388608` |
| <a id="s-f5ed60d501"></a>`inline_native_value_bytes` | `'int'` | `1048576` |
| <a id="s-d224c7f5a1"></a>`maximum_native_value_bytes` | `'int'` | `268435456` |
| <a id="s-c8a55613d7"></a>`large_value_disposition` | `'LargeValueDisposition'` | `<LargeValueDisposition.DIGEST_ONLY: 'digest_only'>` |
| <a id="s-c76382ead6"></a>`capture_xattrs` | `'bool'` | `True` |
| <a id="s-0edf36ac2e"></a>`capture_acl` | `'bool'` | `True` |
| <a id="s-f7cbcc48fd"></a>`capture_file_flags` | `'bool'` | `True` |
| <a id="s-d6739710d4"></a>`capture_sparse_map` | `'bool'` | `True` |
| <a id="s-c7ec07c610"></a>`capture_special_features` | `'bool'` | `True` |
| <a id="s-9971651743"></a>`capture_native_stat` | `'bool'` | `True` |
| <a id="s-1b62365f25"></a>`resolve_principals` | `'bool'` | `True` |
| <a id="s-eb3a04abab"></a>`include_access_time` | `'bool'` | `True` |
| <a id="s-d09499d5f4"></a>`include_hostname` | `'bool'` | `True` |
| <a id="s-811f8255b3"></a>`include_effective_principal` | `'bool'` | `True` |
| <a id="s-c588882ffd"></a>`maximum_sparse_extents` | `'int'` | `100000` |
| <a id="s-392b1a59bf"></a>`resource_fork_chunk_bytes` | `'int'` | `8388608` |
| <a id="s-9e498b3df1"></a>`native_stream_chunk_bytes` | `'int'` | `8388608` |
| <a id="s-98179e9f75"></a>`maximum_native_streams` | `'int'` | `10000` |
| <a id="s-8090d3f334"></a>`capture_system_acl` | `'bool'` | `False` |
| <a id="s-bdda2bcd6b"></a>`windows_allow_shared_write` | `'bool'` | `False` |
| <a id="s-088d6f468f"></a>`windows_allow_shared_delete` | `'bool'` | `False` |
| <a id="s-16ddf42208"></a>`windows_follow_non_name_surrogate_reparse_points` | `'bool'` | `True` |
| <a id="s-5be7047429"></a>`windows_capture_usn` | `'bool'` | `True` |
| <a id="s-c32aa56c9e"></a>`windows_capture_object_id` | `'bool'` | `True` |

## Governing policies

- <a id="pa-cbd2a034eb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.ObservationPolicy`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 49737c37528a1cd29d262541923dc35b557812bec3cd29e6791bf8f21601cced -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "True",
        "name": "strict_consistency",
        "type": "'bool'"
      },
      {
        "default": "True",
        "name": "attempt_noatime",
        "type": "'bool'"
      },
      {
        "default": "True",
        "name": "verify_path_binding",
        "type": "'bool'"
      },
      {
        "default": "False",
        "name": "second_content_hash",
        "type": "'bool'"
      },
      {
        "default": "8388608",
        "name": "hash_chunk_bytes",
        "type": "'int'"
      },
      {
        "default": "1048576",
        "name": "inline_native_value_bytes",
        "type": "'int'"
      },
      {
        "default": "268435456",
        "name": "maximum_native_value_bytes",
        "type": "'int'"
      },
      {
        "default": "<LargeValueDisposition.DIGEST_ONLY: 'digest_only'>",
        "name": "large_value_disposition",
        "type": "'LargeValueDisposition'"
      },
      {
        "default": "True",
        "name": "capture_xattrs",
        "type": "'bool'"
      },
      {
        "default": "True",
        "name": "capture_acl",
        "type": "'bool'"
      },
      {
        "default": "True",
        "name": "capture_file_flags",
        "type": "'bool'"
      },
      {
        "default": "True",
        "name": "capture_sparse_map",
        "type": "'bool'"
      },
      {
        "default": "True",
        "name": "capture_special_features",
        "type": "'bool'"
      },
      {
        "default": "True",
        "name": "capture_native_stat",
        "type": "'bool'"
      },
      {
        "default": "True",
        "name": "resolve_principals",
        "type": "'bool'"
      },
      {
        "default": "True",
        "name": "include_access_time",
        "type": "'bool'"
      },
      {
        "default": "True",
        "name": "include_hostname",
        "type": "'bool'"
      },
      {
        "default": "True",
        "name": "include_effective_principal",
        "type": "'bool'"
      },
      {
        "default": "100000",
        "name": "maximum_sparse_extents",
        "type": "'int'"
      },
      {
        "default": "8388608",
        "name": "resource_fork_chunk_bytes",
        "type": "'int'"
      },
      {
        "default": "8388608",
        "name": "native_stream_chunk_bytes",
        "type": "'int'"
      },
      {
        "default": "10000",
        "name": "maximum_native_streams",
        "type": "'int'"
      },
      {
        "default": "False",
        "name": "capture_system_acl",
        "type": "'bool'"
      },
      {
        "default": "False",
        "name": "windows_allow_shared_write",
        "type": "'bool'"
      },
      {
        "default": "False",
        "name": "windows_allow_shared_delete",
        "type": "'bool'"
      },
      {
        "default": "True",
        "name": "windows_follow_non_name_surrogate_reparse_points",
        "type": "'bool'"
      },
      {
        "default": "True",
        "name": "windows_capture_usn",
        "type": "'bool'"
      },
      {
        "default": "True",
        "name": "windows_capture_object_id",
        "type": "'bool'"
      }
    ],
    "kind": "class",
    "signature": "\"(strict_consistency: 'bool' = True, attempt_noatime: 'bool' = True, verify_path_binding: 'bool' = True, second_content_hash: 'bool' = False, hash_chunk_bytes: 'int' = 8388608, inline_native_value_bytes: 'int' = 1048576, maximum_native_value_bytes: 'int' = 268435456, large_value_disposition: 'LargeValueDisposition' = <LargeValueDisposition.DIGEST_ONLY: 'digest_only'>, capture_xattrs: 'bool' = True, capture_acl: 'bool' = True, capture_file_flags: 'bool' = True, capture_sparse_map: 'bool' = True, capture_special_features: 'bool' = True, capture_native_stat: 'bool' = True, resolve_principals: 'bool' = True, include_access_time: 'bool' = True, include_hostname: 'bool' = True, include_effective_principal: 'bool' = True, maximum_sparse_extents: 'int' = 100000, resource_fork_chunk_bytes: 'int' = 8388608, native_stream_chunk_bytes: 'int' = 8388608, maximum_native_streams: 'int' = 10000, capture_system_acl: 'bool' = False, windows_allow_shared_write: 'bool' = False, windows_allow_shared_delete: 'bool' = False, windows_follow_non_name_surrogate_reparse_points: 'bool' = True, windows_capture_usn: 'bool' = True, windows_capture_object_id: 'bool' = True) -> None\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "ObservationPolicy",
  "unit": "export"
}
```

</details>
