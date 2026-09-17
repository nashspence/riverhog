# gogurt_path_volume_support.PATH_MARKER_NAME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-path-volume-support:gogurt-path-volume-support-path-marker-name:3e3c713ad1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-path-volume-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-02043a19c4"></a>
- <a id="s-df43e215fa"></a>`distribution`: `gogurt-path-volume-support`
- <a id="s-bfa876cdde"></a>`module`: `gogurt_path_volume_support`
- <a id="s-bfee8f6133"></a>`name`: `PATH_MARKER_NAME`
- <a id="s-8d6968559c"></a>`unit`: `export`

### Declared structure

- <a id="s-b089b9000d"></a>`kind`: `"constant"`
- <a id="s-5e1fbf6120"></a>`value`: `".gogurt"`

## Governing policies

- <a id="pa-30d5a9bf6a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-path-volume-support:gogurt_path_volume_support](../../../evidence/sources/authorities.md#src-a67d948855) — [reference/gogurt/mounted-volume/path-support/src/gogurt\_path\_volume\_support/\_\_init\_\_.py](../../../../../../reference/gogurt/mounted-volume/path-support/src/gogurt_path_volume_support/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_path_volume_support.PATH_MARKER_NAME`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1384562054f9b47fe54b9e022ed59295b6a258cd6d3de9db35c359b7eb744823 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": ".gogurt"
  },
  "distribution": "gogurt-path-volume-support",
  "module": "gogurt_path_volume_support",
  "name": "PATH_MARKER_NAME",
  "unit": "export"
}
```

</details>
