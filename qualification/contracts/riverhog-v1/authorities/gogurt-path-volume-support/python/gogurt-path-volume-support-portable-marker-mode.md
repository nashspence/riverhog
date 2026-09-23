# gogurt_path_volume_support.PORTABLE_MARKER_MODE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-path-volume-support:gogurt-path-volume-support-portable-marker-mode:0a378e3524 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-path-volume-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7ec39800a4"></a>
- <a id="s-dc968f426b"></a>`distribution`: `gogurt-path-volume-support`
- <a id="s-8690847334"></a>`module`: `gogurt_path_volume_support`
- <a id="s-5f3d5c0645"></a>`name`: `PORTABLE_MARKER_MODE`
- <a id="s-02352c143c"></a>`unit`: `export`

### Declared structure

- <a id="s-3e433a67fe"></a>`kind`: `"constant"`
- <a id="s-37dae5670d"></a>`value`: `420`

## Governing policies

- <a id="pa-8f268e2e2d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-path-volume-support:gogurt_path_volume_support](../../../evidence/sources/authorities.md#src-a67d948855) — [some-implementations/gogurt/mounted-volume/path-support/src/gogurt\_path\_volume\_support/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/mounted-volume/path-support/src/gogurt_path_volume_support/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_path_volume_support.PORTABLE_MARKER_MODE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9015e01c07c2aa3f950a23a7a4b818437e8f9b39472233d8564a0dc88f157e6f -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 420
  },
  "distribution": "gogurt-path-volume-support",
  "module": "gogurt_path_volume_support",
  "name": "PORTABLE_MARKER_MODE",
  "unit": "export"
}
```

</details>
