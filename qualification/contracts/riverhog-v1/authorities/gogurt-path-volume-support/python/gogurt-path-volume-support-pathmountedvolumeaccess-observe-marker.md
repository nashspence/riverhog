# gogurt_path_volume_support.PathMountedVolumeAccess.observe_marker

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-path-volume-support:gogurt-path-volume-support-pathmountedvol-ed1e01dfc2:e60e2d4f9e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-path-volume-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-15d19c4c66"></a>
- <a id="s-89027a2bae"></a>`distribution`: `gogurt-path-volume-support`
- <a id="s-e31051d64b"></a>`module`: `gogurt_path_volume_support`
- <a id="s-74f7113728"></a>`name`: `observe_marker`
- <a id="s-17bc433432"></a>`owner`: `gogurt_path_volume_support.PathMountedVolumeAccess`
- <a id="s-c9b15fcbae"></a>`unit`: `member`

### Declared structure

- <a id="s-3ced95bc96"></a>`kind`: `"method"`
- <a id="s-7c811e559a"></a>`signature`: `"\"(self, mount_point: 'Path') -> 'MountedMarkerObservation \| None'\""`

## Maintained corroboration

### Related interface records

- [PathMountedVolumeAccess](gogurt-path-volume-support-pathmountedvolumeaccess.md)

## Governing policies

- <a id="pa-3d1ba7c667"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-path-volume-support:gogurt_path_volume_support](../../../evidence/sources.md#src-a67d948855) — `reference/gogurt/mounted-volume/path-support/src/gogurt_path_volume_support/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_path_volume_support.PathMountedVolumeAccess.observe_marker`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 870899acc6ae371ebe5f56e6fca68cd1b3e16ffe49cb0dee7126b4644f4edd4c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, mount_point: 'Path') -> 'MountedMarkerObservation | None'\""
  },
  "distribution": "gogurt-path-volume-support",
  "module": "gogurt_path_volume_support",
  "name": "observe_marker",
  "owner": "gogurt_path_volume_support.PathMountedVolumeAccess",
  "unit": "member"
}
```

</details>
