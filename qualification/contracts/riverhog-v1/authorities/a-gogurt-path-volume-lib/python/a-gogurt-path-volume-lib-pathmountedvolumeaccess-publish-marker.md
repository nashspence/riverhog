# a_gogurt_path_volume_lib.PathMountedVolumeAccess.publish_marker

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-path-volume-lib:a-gogurt-path-volume-lib-pathmountedvolum-f2a793913c:28a43cf94c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-path-volume-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f95352f428"></a>
- <a id="s-dfce6ae9f3"></a>`distribution`: `a-gogurt-path-volume-lib`
- <a id="s-f11a9e5ea6"></a>`module`: `a_gogurt_path_volume_lib`
- <a id="s-5288fb2d2e"></a>`name`: `publish_marker`
- <a id="s-19aeafcfac"></a>`owner`: `a_gogurt_path_volume_lib.PathMountedVolumeAccess`
- <a id="s-b6d44c6a40"></a>`unit`: `member`

### Declared structure

- <a id="s-88ad932c8b"></a>`kind`: `"method"`
- <a id="s-389b11f3c2"></a>`signature`: `"\"(self, mount_point: 'Path', document: 'GogurtRouteMarker', *, expected: 'MountedMarkerObservation \| None') -> 'MountedMarkerObservation'\""`

## Maintained corroboration

### Related interface records

- [PathMountedVolumeAccess](a-gogurt-path-volume-lib-pathmountedvolumeaccess.md)

## Governing policies

- <a id="pa-7e23c13d12"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-path-volume-lib:a_gogurt_path_volume_lib](../../../evidence/sources/authorities.md#src-136f2cd45f) — [some-implementations/gogurt/mounted-volume/path-support/src/a\_gogurt\_path\_volume\_lib/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/mounted-volume/path-support/src/a_gogurt_path_volume_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_path_volume_lib.PathMountedVolumeAccess.publish_marker`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 78c289116b51fda91bcbe1c1ccae7462b7429086a29bbe178638d21b90ed4ceb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, mount_point: 'Path', document: 'GogurtRouteMarker', *, expected: 'MountedMarkerObservation | None') -> 'MountedMarkerObservation'\""
  },
  "distribution": "a-gogurt-path-volume-lib",
  "module": "a_gogurt_path_volume_lib",
  "name": "publish_marker",
  "owner": "a_gogurt_path_volume_lib.PathMountedVolumeAccess",
  "unit": "member"
}
```

</details>
