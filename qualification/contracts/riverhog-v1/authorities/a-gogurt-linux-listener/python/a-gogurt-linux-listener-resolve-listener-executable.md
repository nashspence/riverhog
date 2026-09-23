# a_gogurt_linux_listener.resolve_listener_executable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-linux-listener:a-gogurt-linux-listener-resolve-listener-executable:20428022c2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-linux-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-079c5e931f"></a>
- <a id="s-7527d2d44d"></a>`distribution`: `a-gogurt-linux-listener`
- <a id="s-4d3d029e43"></a>`module`: `a_gogurt_linux_listener`
- <a id="s-82bb1d12f9"></a>`name`: `resolve_listener_executable`
- <a id="s-a212884df0"></a>`unit`: `export`

### Declared structure

- <a id="s-e247b1d624"></a>`kind`: `"function"`
- <a id="s-85a4f20710"></a>`signature`: `"\"(raw: 'str \| None' = None) -> 'Path'\""`

## Governing policies

- <a id="pa-2d285be3aa"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-linux-listener:a_gogurt_linux_listener](../../../evidence/sources/authorities.md#src-5662842a8e) — [some-implementations/gogurt/listener-host/linux/src/a\_gogurt\_linux\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/linux/src/a_gogurt_linux_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_linux_listener.resolve_listener_executable`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 242e10b51d181a636fac1e7bb12fa2735f4f0cb8e40cb12d39f751ffc7aab531 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(raw: 'str | None' = None) -> 'Path'\""
  },
  "distribution": "a-gogurt-linux-listener",
  "module": "a_gogurt_linux_listener",
  "name": "resolve_listener_executable",
  "unit": "export"
}
```

</details>
