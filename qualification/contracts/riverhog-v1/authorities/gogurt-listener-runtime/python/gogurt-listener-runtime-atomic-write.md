# gogurt_listener_runtime.atomic_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-atomic-write:8e59df4501 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e39a94a854"></a>
- <a id="s-59a76ac7ef"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-064f63f81c"></a>`module`: `gogurt_listener_runtime`
- <a id="s-57f2fde26a"></a>`name`: `atomic_write`
- <a id="s-bbc92ae0bb"></a>`unit`: `export`

### Declared structure

- <a id="s-79b46a9bf2"></a>`kind`: `"function"`
- <a id="s-e823be85c4"></a>`signature`: `"\"(destination: 'Path', content: 'bytes', *, mode: 'int') -> 'None'\""`

## Governing policies

- <a id="pa-19146dd9ee"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.atomic_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f889fd2e0f2f0556e70bf555f59a82e5955d44ea303f2113cd030baa7100680f -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(destination: 'Path', content: 'bytes', *, mode: 'int') -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "atomic_write",
  "unit": "export"
}
```

</details>
