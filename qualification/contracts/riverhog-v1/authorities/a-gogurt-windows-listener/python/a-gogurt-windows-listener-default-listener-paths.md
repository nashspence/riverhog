# a_gogurt_windows_listener.default_listener_paths

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-listener:a-gogurt-windows-listener-default-listener-paths:fffa0eb569 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5dad9f6451"></a>
- <a id="s-9238770b05"></a>`distribution`: `a-gogurt-windows-listener`
- <a id="s-4f20f646ea"></a>`module`: `a_gogurt_windows_listener`
- <a id="s-b3c2a6e0cd"></a>`name`: `default_listener_paths`
- <a id="s-accb8b8a3e"></a>`unit`: `export`

### Declared structure

- <a id="s-beb3265d42"></a>`kind`: `"function"`
- <a id="s-e8c9b7b6dd"></a>`signature`: `"\"(*, environment: 'Mapping[str, str] \| None' = None, home: 'Path \| None' = None) -> 'ListenerRuntimePaths'\""`

## Governing policies

- <a id="pa-c89ddc189a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-listener:a_gogurt_windows_listener](../../../evidence/sources/authorities.md#src-29f22753ba) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_listener.default_listener_paths`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 00eff39df1df285879a5c1e477a291937db4e356482495d83355b97587c78e0a -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, environment: 'Mapping[str, str] | None' = None, home: 'Path | None' = None) -> 'ListenerRuntimePaths'\""
  },
  "distribution": "a-gogurt-windows-listener",
  "module": "a_gogurt_windows_listener",
  "name": "default_listener_paths",
  "unit": "export"
}
```

</details>
