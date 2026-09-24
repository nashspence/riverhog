# a_gogurt_path_volume_lib.MAX_PATH_MARKER_BYTES

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-path-volume-lib:a-gogurt-path-volume-lib-max-path-marker-bytes:2c551ca7a6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-path-volume-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-08fed09bd3"></a>
- <a id="s-bec1d3cc0f"></a>`distribution`: `a-gogurt-path-volume-lib`
- <a id="s-941772473a"></a>`module`: `a_gogurt_path_volume_lib`
- <a id="s-c1f4d67c8e"></a>`name`: `MAX_PATH_MARKER_BYTES`
- <a id="s-b0f44f71c8"></a>`unit`: `export`

### Declared structure

- <a id="s-0d27dc30fe"></a>`kind`: `"constant"`
- <a id="s-bc7114ef3b"></a>`value`: `4096`

## Governing policies

- <a id="pa-13d0c5ec8b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-path-volume-lib:a_gogurt_path_volume_lib](../../../evidence/sources/authorities.md#src-136f2cd45f) — [some-implementations/gogurt/mounted-volume/path-support/src/a\_gogurt\_path\_volume\_lib/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/mounted-volume/path-support/src/a_gogurt_path_volume_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_path_volume_lib.MAX_PATH_MARKER_BYTES`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 560ac271b287676a43d8120c92efde2fb8808da1d5a9779cf2941c60557b4eb5 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 4096
  },
  "distribution": "a-gogurt-path-volume-lib",
  "module": "a_gogurt_path_volume_lib",
  "name": "MAX_PATH_MARKER_BYTES",
  "unit": "export"
}
```

</details>
