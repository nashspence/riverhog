# gogurt_linux_listener_host.SystemdUserAdapter.stop

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-linux-listener-host:gogurt-linux-listener-host-systemduseradapter-stop:0099164d74 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7c4d1e4869"></a>
- <a id="s-563ef5cb49"></a>`distribution`: `gogurt-linux-listener-host`
- <a id="s-925b0ff220"></a>`module`: `gogurt_linux_listener_host`
- <a id="s-c857c1be5c"></a>`name`: `stop`
- <a id="s-252183b7bf"></a>`owner`: `gogurt_linux_listener_host.SystemdUserAdapter`
- <a id="s-770b8ee08e"></a>`unit`: `member`

### Declared structure

- <a id="s-560abd3183"></a>`kind`: `"method"`
- <a id="s-1bafd3aa62"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [SystemdUserAdapter](gogurt-linux-listener-host-systemduseradapter.md)

## Governing policies

- <a id="pa-b9f883e1c9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-linux-listener-host:gogurt_linux_listener_host](../../../evidence/sources.md#src-78f263d456) — [reference/gogurt/listener-host/linux/src/gogurt\_linux\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_linux_listener_host.SystemdUserAdapter.stop`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42b569b50034db835dd34500a6c331a30a870914807daddd4a351e1f94b00f5c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "gogurt-linux-listener-host",
  "module": "gogurt_linux_listener_host",
  "name": "stop",
  "owner": "gogurt_linux_listener_host.SystemdUserAdapter",
  "unit": "member"
}
```

</details>
