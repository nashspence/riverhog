# a_gogurt_windows_listener.TaskSchedulerUserAdapter.register

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-listener:a-gogurt-windows-listener-taskschedulerus-5ee847633d:66f4065970 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-47f3ba4d04"></a>
- <a id="s-37690dc07a"></a>`distribution`: `a-gogurt-windows-listener`
- <a id="s-51dcf78ad0"></a>`module`: `a_gogurt_windows_listener`
- <a id="s-14a2a03e42"></a>`name`: `register`
- <a id="s-8c9e5c4d10"></a>`owner`: `a_gogurt_windows_listener.TaskSchedulerUserAdapter`
- <a id="s-2b862e264a"></a>`unit`: `member`

### Declared structure

- <a id="s-28424bdd9b"></a>`kind`: `"method"`
- <a id="s-300d275dc2"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths', command: 'Sequence[str]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [TaskSchedulerUserAdapter](a-gogurt-windows-listener-taskscheduleruseradapter.md)

## Governing policies

- <a id="pa-9bfd71d7ff"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-listener:a_gogurt_windows_listener](../../../evidence/sources/authorities.md#src-29f22753ba) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_listener.TaskSchedulerUserAdapter.register`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b03ea2b38220448c571cc6405fefd1ca6ad0632fb20c9b901813d133bc020eed -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths', command: 'Sequence[str]') -> 'None'\""
  },
  "distribution": "a-gogurt-windows-listener",
  "module": "a_gogurt_windows_listener",
  "name": "register",
  "owner": "a_gogurt_windows_listener.TaskSchedulerUserAdapter",
  "unit": "member"
}
```

</details>
