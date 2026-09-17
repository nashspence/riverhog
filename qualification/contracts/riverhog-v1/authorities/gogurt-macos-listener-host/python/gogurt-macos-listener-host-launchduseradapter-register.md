# gogurt_macos_listener_host.LaunchdUserAdapter.register

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-macos-listener-host:gogurt-macos-listener-host-launchduserada-4e70a13278:335162b388 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-macos-listener-host](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-02546c34a9"></a>
- <a id="s-53d4e7893f"></a>`distribution`: `gogurt-macos-listener-host`
- <a id="s-4390021a5a"></a>`module`: `gogurt_macos_listener_host`
- <a id="s-c60cb1cb0b"></a>`name`: `register`
- <a id="s-1d320756ed"></a>`owner`: `gogurt_macos_listener_host.LaunchdUserAdapter`
- <a id="s-fa49a942d1"></a>`unit`: `member`

### Declared structure

- <a id="s-6415378597"></a>`kind`: `"method"`
- <a id="s-5614435ce3"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths', command: 'Sequence[str]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [LaunchdUserAdapter](gogurt-macos-listener-host-launchduseradapter.md)

## Governing policies

- <a id="pa-4402fdc0b8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-macos-listener-host:gogurt_macos_listener_host](../../../evidence/sources/authorities.md#src-3a09f7fc10) — [reference/gogurt/listener-host/macos/src/gogurt\_macos\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/macos/src/gogurt_macos_listener_host/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_macos_listener_host.LaunchdUserAdapter.register`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6cdc20754e0d844a69b80f05c12363b9c5b82fae92cdd3ed5b2ff03b90c2c24c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths', command: 'Sequence[str]') -> 'None'\""
  },
  "distribution": "gogurt-macos-listener-host",
  "module": "gogurt_macos_listener_host",
  "name": "register",
  "owner": "gogurt_macos_listener_host.LaunchdUserAdapter",
  "unit": "member"
}
```

</details>
