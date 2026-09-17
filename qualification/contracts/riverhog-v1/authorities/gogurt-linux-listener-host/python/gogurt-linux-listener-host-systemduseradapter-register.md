# gogurt_linux_listener_host.SystemdUserAdapter.register

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-listener-host:gogurt-linux-listener-host-systemduserada-8ad3e6b4b4:cf9f47c14a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2ee3115686"></a>
- <a id="s-4e68e456b9"></a>`distribution`: `gogurt-linux-listener-host`
- <a id="s-099bf7c704"></a>`module`: `gogurt_linux_listener_host`
- <a id="s-2d05d5a3c3"></a>`name`: `register`
- <a id="s-4d8720d90d"></a>`owner`: `gogurt_linux_listener_host.SystemdUserAdapter`
- <a id="s-bd83b27e3a"></a>`unit`: `member`

### Declared structure

- <a id="s-5735baf121"></a>`kind`: `"method"`
- <a id="s-8110766e4d"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths', command: 'Sequence[str]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [SystemdUserAdapter](gogurt-linux-listener-host-systemduseradapter.md)

## Governing policies

- <a id="pa-ac059137c3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-linux-listener-host:gogurt_linux_listener_host](../../../evidence/sources/authorities.md#src-78f263d456) — [reference/gogurt/listener-host/linux/src/gogurt\_linux\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_linux_listener_host.SystemdUserAdapter.register`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 15d728c0ae31b3f3da108c7cec4ecdfb311cd013175e0539848c4e248c9ba592 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths', command: 'Sequence[str]') -> 'None'\""
  },
  "distribution": "gogurt-linux-listener-host",
  "module": "gogurt_linux_listener_host",
  "name": "register",
  "owner": "gogurt_linux_listener_host.SystemdUserAdapter",
  "unit": "member"
}
```

</details>
