# gogurt_listener_runtime.run_listener

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-run-listener:74ec87e59b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8c1d401aef"></a>
- <a id="s-eec0b6f86d"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-31d4ff91f3"></a>`module`: `gogurt_listener_runtime`
- <a id="s-79bf859d27"></a>`name`: `run_listener`
- <a id="s-7362adb1e3"></a>`unit`: `export`

### Declared structure

- <a id="s-facbdc974a"></a>`kind`: `"function"`
- <a id="s-6ec71ce268"></a>`signature`: `"\"(config_file: 'Path', *, mounted_volume_provider: 'MountedVolumeProvider', product_version: 'str') -> 'None'\""`

## Governing policies

- <a id="pa-b36877128f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.run_listener`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b0cb19ab8cccc822fa5f036a856f9a1430c10c243af2e5c64b5886a42c84c827 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(config_file: 'Path', *, mounted_volume_provider: 'MountedVolumeProvider', product_version: 'str') -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "run_listener",
  "unit": "export"
}
```

</details>
