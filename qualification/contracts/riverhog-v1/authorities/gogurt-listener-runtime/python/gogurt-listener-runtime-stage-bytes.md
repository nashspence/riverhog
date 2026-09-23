# gogurt_listener_runtime.stage_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-stage-bytes:43b51e0bae -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-36292677f9"></a>
- <a id="s-a4a98352c8"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-7ec6c31c51"></a>`module`: `gogurt_listener_runtime`
- <a id="s-1874271e48"></a>`name`: `stage_bytes`
- <a id="s-78de8b662d"></a>`unit`: `export`

### Declared structure

- <a id="s-e853bcf80d"></a>`kind`: `"function"`
- <a id="s-b21f1b7c43"></a>`signature`: `"\"(destination: 'Path', content: 'bytes', *, mode: 'int') -> 'Path'\""`

## Governing policies

- <a id="pa-6e59306290"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.stage_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 44de47271dc6384d5f42b315e459a74786a00d3e7fe2d60bed5707ce43bec553 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(destination: 'Path', content: 'bytes', *, mode: 'int') -> 'Path'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "stage_bytes",
  "unit": "export"
}
```

</details>
