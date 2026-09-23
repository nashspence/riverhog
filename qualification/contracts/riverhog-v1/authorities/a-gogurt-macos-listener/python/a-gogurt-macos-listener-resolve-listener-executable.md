# a_gogurt_macos_listener.resolve_listener_executable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-macos-listener:a-gogurt-macos-listener-resolve-listener-executable:4807032b78 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-macos-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a929010ddd"></a>
- <a id="s-897d37bb8e"></a>`distribution`: `a-gogurt-macos-listener`
- <a id="s-a61e2997e5"></a>`module`: `a_gogurt_macos_listener`
- <a id="s-e7aee560bf"></a>`name`: `resolve_listener_executable`
- <a id="s-304b506bec"></a>`unit`: `export`

### Declared structure

- <a id="s-14aada21b0"></a>`kind`: `"function"`
- <a id="s-9cff807d41"></a>`signature`: `"\"(raw: 'str \| None' = None) -> 'Path'\""`

## Governing policies

- <a id="pa-8a09fadd54"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-macos-listener:a_gogurt_macos_listener](../../../evidence/sources/authorities.md#src-94f2d0391f) — [some-implementations/gogurt/listener-host/macos/src/a\_gogurt\_macos\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/macos/src/a_gogurt_macos_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_macos_listener.resolve_listener_executable`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6131f41947354574ba10a419f0bff6670c0a0f7e9e8f2c83e913c404f5a18711 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(raw: 'str | None' = None) -> 'Path'\""
  },
  "distribution": "a-gogurt-macos-listener",
  "module": "a_gogurt_macos_listener",
  "name": "resolve_listener_executable",
  "unit": "export"
}
```

</details>
