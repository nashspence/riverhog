# gogurt_listener_runtime.ensure_private_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-ensure-private-files:88e52647b1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e0113ce00f"></a>
- <a id="s-cd809d170d"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-f927ca26f7"></a>`module`: `gogurt_listener_runtime`
- <a id="s-cecad989d8"></a>`name`: `ensure_private_files`
- <a id="s-e25912a7e6"></a>`unit`: `export`

### Declared structure

- <a id="s-137c3dc53f"></a>`kind`: `"function"`
- <a id="s-dae56a65f0"></a>`signature`: `"\"(paths: 'Iterable[Path]') -> 'None'\""`

## Governing policies

- <a id="pa-8d46e1589d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ensure_private_files`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cc1d22713d84a43e188cefcaf1cd37df1c93152a0062894317d9c66ce102f875 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(paths: 'Iterable[Path]') -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "ensure_private_files",
  "unit": "export"
}
```

</details>
