# a_gogurt_macos_listener.render_launchd_plist

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-macos-listener:a-gogurt-macos-listener-render-launchd-plist:8a2c3f6cee -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-macos-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a033d188f1"></a>
- <a id="s-6521812d05"></a>`distribution`: `a-gogurt-macos-listener`
- <a id="s-337f36ca10"></a>`module`: `a_gogurt_macos_listener`
- <a id="s-06a8687260"></a>`name`: `render_launchd_plist`
- <a id="s-3c5e5651eb"></a>`unit`: `export`

### Declared structure

- <a id="s-b56ba6c23e"></a>`kind`: `"function"`
- <a id="s-58be095b22"></a>`signature`: `"\"(command: 'Sequence[str]') -> 'bytes'\""`

## Governing policies

- <a id="pa-e5af2a7bdf"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-macos-listener:a_gogurt_macos_listener](../../../evidence/sources/authorities.md#src-94f2d0391f) — [some-implementations/gogurt/listener-host/macos/src/a\_gogurt\_macos\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/macos/src/a_gogurt_macos_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_macos_listener.render_launchd_plist`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 299f8dda07fef06d66b12f77189f751acf8481fca0378f3adc318ab4ec082ea3 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(command: 'Sequence[str]') -> 'bytes'\""
  },
  "distribution": "a-gogurt-macos-listener",
  "module": "a_gogurt_macos_listener",
  "name": "render_launchd_plist",
  "unit": "export"
}
```

</details>
