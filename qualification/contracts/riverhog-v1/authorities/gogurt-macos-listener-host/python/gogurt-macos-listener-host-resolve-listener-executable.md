# gogurt_macos_listener_host.resolve_listener_executable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-macos-listener-host:gogurt-macos-listener-host-resolve-listen-1f2c32a4df:31602ab947 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b1e8e9d809"></a>
- <a id="s-bc63ee96e0"></a>`distribution`: `gogurt-macos-listener-host`
- <a id="s-4f2d60b739"></a>`module`: `gogurt_macos_listener_host`
- <a id="s-461a3cfd83"></a>`name`: `resolve_listener_executable`
- <a id="s-125fc24c53"></a>`unit`: `export`

### Declared structure

- <a id="s-ff89eda523"></a>`kind`: `"function"`
- <a id="s-64e7d48a30"></a>`signature`: `"\"(raw: 'str \| None' = None) -> 'Path'\""`

## Governing policies

- <a id="pa-2e67c1642e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-macos-listener-host:gogurt_macos_listener_host](../../../evidence/sources/authorities.md#src-3a09f7fc10) — [reference/gogurt/listener-host/macos/src/gogurt\_macos\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/macos/src/gogurt_macos_listener_host/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_macos_listener_host.resolve_listener_executable`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2d007e12f2b75e844c856c04f002b273b42002690b99ce3c30e6ee6162bebd7d -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(raw: 'str | None' = None) -> 'Path'\""
  },
  "distribution": "gogurt-macos-listener-host",
  "module": "gogurt_macos_listener_host",
  "name": "resolve_listener_executable",
  "unit": "export"
}
```

</details>
