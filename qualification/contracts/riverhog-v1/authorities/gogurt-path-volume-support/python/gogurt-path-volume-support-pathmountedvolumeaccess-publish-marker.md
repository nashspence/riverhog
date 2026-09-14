# gogurt_path_volume_support.PathMountedVolumeAccess.publish_marker

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-path-volume-support:gogurt-path-volume-support-pathmountedvol-94844608c8:40897f9d70 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-path-volume-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6414150756"></a>
- <a id="s-045575f83c"></a>`distribution`: `gogurt-path-volume-support`
- <a id="s-6089081775"></a>`module`: `gogurt_path_volume_support`
- <a id="s-c25820616f"></a>`name`: `publish_marker`
- <a id="s-613b16be79"></a>`owner`: `gogurt_path_volume_support.PathMountedVolumeAccess`
- <a id="s-1c36f58117"></a>`unit`: `member`

### Declared structure

- <a id="s-f742a73e54"></a>`kind`: `"method"`
- <a id="s-ffebf42416"></a>`signature`: `"\"(self, mount_point: 'Path', document: 'GogurtRouteMarker', *, expected: 'MountedMarkerObservation \| None') -> 'MountedMarkerObservation'\""`

## Maintained corroboration

### Related interface records

- [gogurt_path_volume_support.PathMountedVolumeAccess](gogurt-path-volume-support-pathmountedvolumeaccess.md)

## Governing policies

- <a id="pa-1cd73e44b1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-path-volume-support:gogurt_path_volume_support](../../../evidence/sources.md#src-a67d948855) — `reference/gogurt/mounted-volume/path-support/src/gogurt_path_volume_support/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_path_volume_support.PathMountedVolumeAccess.publish_marker`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4b05180e87696bc013c23dbb0b40291d5f181efc5acf32cc560c1c6a74ca1a64 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, mount_point: 'Path', document: 'GogurtRouteMarker', *, expected: 'MountedMarkerObservation | None') -> 'MountedMarkerObservation'\""
  },
  "distribution": "gogurt-path-volume-support",
  "module": "gogurt_path_volume_support",
  "name": "publish_marker",
  "owner": "gogurt_path_volume_support.PathMountedVolumeAccess",
  "unit": "member"
}
```
