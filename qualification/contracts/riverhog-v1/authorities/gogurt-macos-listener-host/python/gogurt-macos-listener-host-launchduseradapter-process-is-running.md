# gogurt_macos_listener_host.LaunchdUserAdapter.process_is_running

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-macos-listener-host:gogurt-macos-listener-host-launchduserada-61643152c4:c1b390693c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3bf64af3e4"></a>
- <a id="s-fe615e88f0"></a>`distribution`: `gogurt-macos-listener-host`
- <a id="s-19e760cb36"></a>`module`: `gogurt_macos_listener_host`
- <a id="s-e744f2d4fa"></a>`name`: `process_is_running`
- <a id="s-56439d051f"></a>`owner`: `gogurt_macos_listener_host.LaunchdUserAdapter`
- <a id="s-1dcb887d6d"></a>`unit`: `member`

### Declared structure

- <a id="s-f11a668db3"></a>`kind`: `"staticmethod"`
- <a id="s-25071ed540"></a>`signature`: `"\"(pid: 'int') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [LaunchdUserAdapter](gogurt-macos-listener-host-launchduseradapter.md)

## Governing policies

- <a id="pa-9351717926"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-macos-listener-host:gogurt_macos_listener_host](../../../evidence/sources/authorities.md#src-3a09f7fc10) — [reference/gogurt/listener-host/macos/src/gogurt\_macos\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/macos/src/gogurt_macos_listener_host/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_macos_listener_host.LaunchdUserAdapter.process_is_running`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7ea5250c899a73a7722436abe0b17fa027f4d496e76a88749f4656ac9d68aa11 -->

```json
{
  "contract": {
    "kind": "staticmethod",
    "signature": "\"(pid: 'int') -> 'bool'\""
  },
  "distribution": "gogurt-macos-listener-host",
  "module": "gogurt_macos_listener_host",
  "name": "process_is_running",
  "owner": "gogurt_macos_listener_host.LaunchdUserAdapter",
  "unit": "member"
}
```

</details>
