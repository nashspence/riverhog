# a_gogurt_windows_listener.resolve_listener_executable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-listener:a-gogurt-windows-listener-resolve-listene-0db5f659a7:dfa2053728 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a7e1fd4fbe"></a>
- <a id="s-2f1d62c5ac"></a>`distribution`: `a-gogurt-windows-listener`
- <a id="s-dc23186769"></a>`module`: `a_gogurt_windows_listener`
- <a id="s-488537f7e7"></a>`name`: `resolve_listener_executable`
- <a id="s-b86dd05a6b"></a>`unit`: `export`

### Declared structure

- <a id="s-a2c15cee99"></a>`kind`: `"function"`
- <a id="s-4a8c7036f6"></a>`signature`: `"\"(raw: 'str \| None' = None) -> 'Path'\""`

## Governing policies

- <a id="pa-76724218ad"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-listener:a_gogurt_windows_listener](../../../evidence/sources/authorities.md#src-29f22753ba) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_listener.resolve_listener_executable`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 159ff650aa0e95aaf9daf532ef569f11d08c600721cfc8b338a2c3bdd058586d -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(raw: 'str | None' = None) -> 'Path'\""
  },
  "distribution": "a-gogurt-windows-listener",
  "module": "a_gogurt_windows_listener",
  "name": "resolve_listener_executable",
  "unit": "export"
}
```

</details>
