# gogurt_linux_listener_host.SystemdUserAdapter.status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-listener-host:gogurt-linux-listener-host-systemduseradapter-status:5849ba456d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a5786efa37"></a>
- <a id="s-745d56619f"></a>`distribution`: `gogurt-linux-listener-host`
- <a id="s-605c50b1e2"></a>`module`: `gogurt_linux_listener_host`
- <a id="s-90fe141e90"></a>`name`: `status`
- <a id="s-8ed525654d"></a>`owner`: `gogurt_linux_listener_host.SystemdUserAdapter`
- <a id="s-7213d45ca9"></a>`unit`: `member`

### Declared structure

- <a id="s-4d3b25c2c1"></a>`kind`: `"method"`
- <a id="s-bfc65129e4"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'NativeListenerStatus'\""`

## Maintained corroboration

### Related interface records

- [SystemdUserAdapter](gogurt-linux-listener-host-systemduseradapter.md)

## Governing policies

- <a id="pa-384413af13"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-linux-listener-host:gogurt_linux_listener_host](../../../evidence/sources/authorities.md#src-78f263d456) — [reference/gogurt/listener-host/linux/src/gogurt\_linux\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_linux_listener_host.SystemdUserAdapter.status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd693a9553d803312a41dacb76dda977553535c7586caff2f0a552ba92fc5577 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'NativeListenerStatus'\""
  },
  "distribution": "gogurt-linux-listener-host",
  "module": "gogurt_linux_listener_host",
  "name": "status",
  "owner": "gogurt_linux_listener_host.SystemdUserAdapter",
  "unit": "member"
}
```

</details>
