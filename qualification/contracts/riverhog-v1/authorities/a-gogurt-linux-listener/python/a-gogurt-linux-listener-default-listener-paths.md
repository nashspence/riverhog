# a_gogurt_linux_listener.default_listener_paths

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-linux-listener:a-gogurt-linux-listener-default-listener-paths:0b74fd24f9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-linux-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-35c60f6c75"></a>
- <a id="s-3fe3cb92d0"></a>`distribution`: `a-gogurt-linux-listener`
- <a id="s-a9e3662fc3"></a>`module`: `a_gogurt_linux_listener`
- <a id="s-e0298d4a86"></a>`name`: `default_listener_paths`
- <a id="s-9c4cec8e36"></a>`unit`: `export`

### Declared structure

- <a id="s-641580565d"></a>`kind`: `"function"`
- <a id="s-7b0934cb02"></a>`signature`: `"\"(*, environment: 'Mapping[str, str] \| None' = None, home: 'Path \| None' = None) -> 'ListenerRuntimePaths'\""`

## Governing policies

- <a id="pa-4c67a8a0fa"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-linux-listener:a_gogurt_linux_listener](../../../evidence/sources/authorities.md#src-5662842a8e) — [some-implementations/gogurt/listener-host/linux/src/a\_gogurt\_linux\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/linux/src/a_gogurt_linux_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_linux_listener.default_listener_paths`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 71c91ac0b0bedbf3ce2457ae633bdd683cbe573d09fab4eb7a87e9208325a9e2 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, environment: 'Mapping[str, str] | None' = None, home: 'Path | None' = None) -> 'ListenerRuntimePaths'\""
  },
  "distribution": "a-gogurt-linux-listener",
  "module": "a_gogurt_linux_listener",
  "name": "default_listener_paths",
  "unit": "export"
}
```

</details>
