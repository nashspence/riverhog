# a_gogurt_path_volume_lib.PathMountedVolumeAccess.observe_marker

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-path-volume-lib:a-gogurt-path-volume-lib-pathmountedvolum-9babe46eaf:6da952d53e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-path-volume-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a4c1dfe4dc"></a>
- <a id="s-51eeff11e4"></a>`distribution`: `a-gogurt-path-volume-lib`
- <a id="s-04847ba2e0"></a>`module`: `a_gogurt_path_volume_lib`
- <a id="s-447604e481"></a>`name`: `observe_marker`
- <a id="s-adb895b0bd"></a>`owner`: `a_gogurt_path_volume_lib.PathMountedVolumeAccess`
- <a id="s-1a77569f56"></a>`unit`: `member`

### Declared structure

- <a id="s-772164f6b8"></a>`kind`: `"method"`
- <a id="s-1c544e6167"></a>`signature`: `"\"(self, mount_point: 'Path') -> 'MountedMarkerObservation \| None'\""`

## Maintained corroboration

### Related interface records

- [PathMountedVolumeAccess](a-gogurt-path-volume-lib-pathmountedvolumeaccess.md)

## Governing policies

- <a id="pa-e374649aa5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-path-volume-lib:a_gogurt_path_volume_lib](../../../evidence/sources/authorities.md#src-136f2cd45f) — [some-implementations/gogurt/mounted-volume/path-support/src/a\_gogurt\_path\_volume\_lib/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/mounted-volume/path-support/src/a_gogurt_path_volume_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_path_volume_lib.PathMountedVolumeAccess.observe_marker`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c140b2a8ec08b67ce84ccead8098563b2ee76abd3156db1255e63a0ce0753443 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, mount_point: 'Path') -> 'MountedMarkerObservation | None'\""
  },
  "distribution": "a-gogurt-path-volume-lib",
  "module": "a_gogurt_path_volume_lib",
  "name": "observe_marker",
  "owner": "a_gogurt_path_volume_lib.PathMountedVolumeAccess",
  "unit": "member"
}
```

</details>
