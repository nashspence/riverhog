# a_gogurt_path_volume_lib.PathMountedVolumeAccess.discover

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-path-volume-lib:a-gogurt-path-volume-lib-pathmountedvolum-2ac3b94472:cae47fc853 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-path-volume-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b14839d259"></a>
- <a id="s-05daa86395"></a>`distribution`: `a-gogurt-path-volume-lib`
- <a id="s-a3fa9464b5"></a>`module`: `a_gogurt_path_volume_lib`
- <a id="s-9cf5417c80"></a>`name`: `discover`
- <a id="s-a4fa09ff6f"></a>`owner`: `a_gogurt_path_volume_lib.PathMountedVolumeAccess`
- <a id="s-8b83ed1ce9"></a>`unit`: `member`

### Declared structure

- <a id="s-843c0e067e"></a>`kind`: `"method"`
- <a id="s-daca140ba0"></a>`signature`: `"\"(self) -> 'Sequence[Path]'\""`

## Maintained corroboration

### Related interface records

- [PathMountedVolumeAccess](a-gogurt-path-volume-lib-pathmountedvolumeaccess.md)

## Governing policies

- <a id="pa-f79bb43c24"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-path-volume-lib:a_gogurt_path_volume_lib](../../../evidence/sources/authorities.md#src-136f2cd45f) — [some-implementations/gogurt/mounted-volume/path-support/src/a\_gogurt\_path\_volume\_lib/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/mounted-volume/path-support/src/a_gogurt_path_volume_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_path_volume_lib.PathMountedVolumeAccess.discover`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9b99fa5d8f215e817f4d253319478beaee1d2e6156f2991e48f601c6822ef209 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Sequence[Path]'\""
  },
  "distribution": "a-gogurt-path-volume-lib",
  "module": "a_gogurt_path_volume_lib",
  "name": "discover",
  "owner": "a_gogurt_path_volume_lib.PathMountedVolumeAccess",
  "unit": "member"
}
```

</details>
