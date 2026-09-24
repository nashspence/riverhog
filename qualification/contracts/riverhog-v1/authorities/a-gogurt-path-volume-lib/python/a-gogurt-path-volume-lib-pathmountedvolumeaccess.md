# a_gogurt_path_volume_lib.PathMountedVolumeAccess

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-path-volume-lib:a-gogurt-path-volume-lib-pathmountedvolumeaccess:797c8b5479 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-path-volume-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-016289514d"></a>
- <a id="s-aebf5a6f08"></a>`distribution`: `a-gogurt-path-volume-lib`
- <a id="s-bd21f8dff5"></a>`module`: `a_gogurt_path_volume_lib`
- <a id="s-6498767b77"></a>`name`: `PathMountedVolumeAccess`
- <a id="s-89d003f6ab"></a>`unit`: `export`

### Declared structure

- <a id="s-4898b7f598"></a>`kind`: `"class"`
- <a id="s-08f2e353a0"></a>`signature`: `"\"(discover_mounts: 'Callable[[], Sequence[Path]]') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-fc4e789f50"></a>`discover_mounts` | `'Callable[[], Sequence[Path]]'` | `required` |

## Maintained corroboration

### Related interface records

- [discover](a-gogurt-path-volume-lib-pathmountedvolumeaccess-discover.md)
- [observe_marker](a-gogurt-path-volume-lib-pathmountedvolumeaccess-observe-marker.md)
- [publish_marker](a-gogurt-path-volume-lib-pathmountedvolumeaccess-publish-marker.md)

## Governing policies

- <a id="pa-d6dce07e78"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-path-volume-lib:a_gogurt_path_volume_lib](../../../evidence/sources/authorities.md#src-136f2cd45f) — [some-implementations/gogurt/mounted-volume/path-support/src/a\_gogurt\_path\_volume\_lib/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/mounted-volume/path-support/src/a_gogurt_path_volume_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_path_volume_lib.PathMountedVolumeAccess`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 85f774225cf36c46a8c4eb363496e543bd316f71de1fc55b4b057e8692efd753 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "discover_mounts",
        "type": "'Callable[[], Sequence[Path]]'"
      }
    ],
    "kind": "class",
    "signature": "\"(discover_mounts: 'Callable[[], Sequence[Path]]') -> None\""
  },
  "distribution": "a-gogurt-path-volume-lib",
  "module": "a_gogurt_path_volume_lib",
  "name": "PathMountedVolumeAccess",
  "unit": "export"
}
```

</details>
