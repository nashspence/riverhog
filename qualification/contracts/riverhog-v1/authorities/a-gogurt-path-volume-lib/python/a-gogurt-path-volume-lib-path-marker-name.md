# a_gogurt_path_volume_lib.PATH_MARKER_NAME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-path-volume-lib:a-gogurt-path-volume-lib-path-marker-name:72b713a8ee -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-path-volume-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-691d4188df"></a>
- <a id="s-7707ae8916"></a>`distribution`: `a-gogurt-path-volume-lib`
- <a id="s-3cb153af40"></a>`module`: `a_gogurt_path_volume_lib`
- <a id="s-305755ab0e"></a>`name`: `PATH_MARKER_NAME`
- <a id="s-79169ca68b"></a>`unit`: `export`

### Declared structure

- <a id="s-c683418e10"></a>`kind`: `"constant"`
- <a id="s-a5cce0a0a5"></a>`value`: `".gogurt"`

## Governing policies

- <a id="pa-377311fb5b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-path-volume-lib:a_gogurt_path_volume_lib](../../../evidence/sources/authorities.md#src-136f2cd45f) — [some-implementations/gogurt/mounted-volume/path-support/src/a\_gogurt\_path\_volume\_lib/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/mounted-volume/path-support/src/a_gogurt_path_volume_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_path_volume_lib.PATH_MARKER_NAME`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a93f0568792856e16eb6ccfe967c37d47fcd656173f0a0ce740547426c5880bb -->

```json
{
  "contract": {
    "kind": "constant",
    "value": ".gogurt"
  },
  "distribution": "a-gogurt-path-volume-lib",
  "module": "a_gogurt_path_volume_lib",
  "name": "PATH_MARKER_NAME",
  "unit": "export"
}
```

</details>
