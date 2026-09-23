# a_gogurt_linux_listener.render_systemd_unit

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-linux-listener:a-gogurt-linux-listener-render-systemd-unit:6ced878e39 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-linux-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-35868045e6"></a>
- <a id="s-2f661263ea"></a>`distribution`: `a-gogurt-linux-listener`
- <a id="s-a699cf007b"></a>`module`: `a_gogurt_linux_listener`
- <a id="s-08bc35b3c7"></a>`name`: `render_systemd_unit`
- <a id="s-a600c85c8a"></a>`unit`: `export`

### Declared structure

- <a id="s-6966e0833e"></a>`kind`: `"function"`
- <a id="s-3062453ac2"></a>`signature`: `"\"(command: 'Sequence[str]') -> 'bytes'\""`

## Governing policies

- <a id="pa-fdc33aa8ab"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-linux-listener:a_gogurt_linux_listener](../../../evidence/sources/authorities.md#src-5662842a8e) — [some-implementations/gogurt/listener-host/linux/src/a\_gogurt\_linux\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/linux/src/a_gogurt_linux_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_linux_listener.render_systemd_unit`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: faa01468eaa747b65c27e142e0f3d71bb6c87284aa143642b2a8963bed101c81 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(command: 'Sequence[str]') -> 'bytes'\""
  },
  "distribution": "a-gogurt-linux-listener",
  "module": "a_gogurt_linux_listener",
  "name": "render_systemd_unit",
  "unit": "export"
}
```

</details>
