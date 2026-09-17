# gogurt_linux_listener_host.SystemdUserAdapter.start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-listener-host:gogurt-linux-listener-host-systemduseradapter-start:fd2a4497c9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-017e9fc5fa"></a>
- <a id="s-4082bd68c9"></a>`distribution`: `gogurt-linux-listener-host`
- <a id="s-82f0b93931"></a>`module`: `gogurt_linux_listener_host`
- <a id="s-4531cba3df"></a>`name`: `start`
- <a id="s-70cdced4c3"></a>`owner`: `gogurt_linux_listener_host.SystemdUserAdapter`
- <a id="s-3c83f4af8c"></a>`unit`: `member`

### Declared structure

- <a id="s-0ec3b8a9d6"></a>`kind`: `"method"`
- <a id="s-e1f57065ae"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [SystemdUserAdapter](gogurt-linux-listener-host-systemduseradapter.md)

## Governing policies

- <a id="pa-d61094435f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-linux-listener-host:gogurt_linux_listener_host](../../../evidence/sources/authorities.md#src-78f263d456) — [reference/gogurt/listener-host/linux/src/gogurt\_linux\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_linux_listener_host.SystemdUserAdapter.start`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: da938bc7fa6bb717686bca8ac5e0567230e828196f64e5b51ab88578d49ff178 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "gogurt-linux-listener-host",
  "module": "gogurt_linux_listener_host",
  "name": "start",
  "owner": "gogurt_linux_listener_host.SystemdUserAdapter",
  "unit": "member"
}
```

</details>
