# a_gogurt_windows_listener.TaskSchedulerUserAdapter.start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-listener:a-gogurt-windows-listener-taskschedulerus-5f9aba5c0e:1dcb2af9e1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2e25cb8043"></a>
- <a id="s-8093eed5dc"></a>`distribution`: `a-gogurt-windows-listener`
- <a id="s-756672c150"></a>`module`: `a_gogurt_windows_listener`
- <a id="s-9b0ec895a4"></a>`name`: `start`
- <a id="s-92fce689c0"></a>`owner`: `a_gogurt_windows_listener.TaskSchedulerUserAdapter`
- <a id="s-5b371b85b9"></a>`unit`: `member`

### Declared structure

- <a id="s-3fcdca5158"></a>`kind`: `"method"`
- <a id="s-59ac07d098"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [TaskSchedulerUserAdapter](a-gogurt-windows-listener-taskscheduleruseradapter.md)

## Governing policies

- <a id="pa-49e03e32a0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-listener:a_gogurt_windows_listener](../../../evidence/sources/authorities.md#src-29f22753ba) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_listener.TaskSchedulerUserAdapter.start`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6ee4a98c6395138621046348554732652ed98c9b38bf3433b5c481991128fa9b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "a-gogurt-windows-listener",
  "module": "a_gogurt_windows_listener",
  "name": "start",
  "owner": "a_gogurt_windows_listener.TaskSchedulerUserAdapter",
  "unit": "member"
}
```

</details>
