# a_gogurt_windows_listener.listener_adapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-listener:a-gogurt-windows-listener-listener-adapter:5843a19b01 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-37f9352786"></a>
- <a id="s-00c39b75d0"></a>`distribution`: `a-gogurt-windows-listener`
- <a id="s-dd3b2aff7c"></a>`module`: `a_gogurt_windows_listener`
- <a id="s-84e2eb5ba5"></a>`name`: `listener_adapter`
- <a id="s-20e77596bb"></a>`unit`: `export`

### Declared structure

- <a id="s-57a3aa38c1"></a>`kind`: `"function"`
- <a id="s-73bb7d925f"></a>`signature`: `"\"() -> 'ListenerAdapter'\""`

## Governing policies

- <a id="pa-b11dff1ae5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-listener:a_gogurt_windows_listener](../../../evidence/sources/authorities.md#src-29f22753ba) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_listener.listener_adapter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 43ddfcf7f02558877e127ea690876676de01ff8e123e2f043bc106698a4ede89 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'ListenerAdapter'\""
  },
  "distribution": "a-gogurt-windows-listener",
  "module": "a_gogurt_windows_listener",
  "name": "listener_adapter",
  "unit": "export"
}
```

</details>
