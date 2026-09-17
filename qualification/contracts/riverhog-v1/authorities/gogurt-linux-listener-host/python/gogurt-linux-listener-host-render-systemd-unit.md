# gogurt_linux_listener_host.render_systemd_unit

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-listener-host:gogurt-linux-listener-host-render-systemd-unit:73923df1d7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cbd8173a9d"></a>
- <a id="s-1e086694a1"></a>`distribution`: `gogurt-linux-listener-host`
- <a id="s-823cc552f5"></a>`module`: `gogurt_linux_listener_host`
- <a id="s-c0ca252a69"></a>`name`: `render_systemd_unit`
- <a id="s-e770c1d8c2"></a>`unit`: `export`

### Declared structure

- <a id="s-770cfb76c9"></a>`kind`: `"function"`
- <a id="s-04346f0b04"></a>`signature`: `"\"(command: 'Sequence[str]') -> 'bytes'\""`

## Governing policies

- <a id="pa-12cc1936f4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-linux-listener-host:gogurt_linux_listener_host](../../../evidence/sources/authorities.md#src-78f263d456) — [reference/gogurt/listener-host/linux/src/gogurt\_linux\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_linux_listener_host.render_systemd_unit`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e2ec61e891e1c852333fa9f1d4be6e31598a78617fa8fe23b69b32cb34b771fa -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(command: 'Sequence[str]') -> 'bytes'\""
  },
  "distribution": "gogurt-linux-listener-host",
  "module": "gogurt_linux_listener_host",
  "name": "render_systemd_unit",
  "unit": "export"
}
```

</details>
