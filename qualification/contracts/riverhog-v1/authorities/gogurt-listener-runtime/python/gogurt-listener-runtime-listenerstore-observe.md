# gogurt_listener_runtime.ListenerStore.observe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerstore-observe:27d522adbc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bd5a7ef2b5"></a>
- <a id="s-e6ce9a7a9a"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-121c9ca41b"></a>`module`: `gogurt_listener_runtime`
- <a id="s-1b6a19b935"></a>`name`: `observe`
- <a id="s-fb6eeeac92"></a>`owner`: `gogurt_listener_runtime.ListenerStore`
- <a id="s-801a673624"></a>`unit`: `member`

### Declared structure

- <a id="s-41018ec30e"></a>`kind`: `"method"`
- <a id="s-e9e72eaf61"></a>`signature`: `"\"(self, mount_points: 'Sequence[Path]', planner: 'Callable[[Path], Mapping[str, object]]', *, now: 'float') -> 'list[str]'\""`

## Maintained corroboration

### Related interface records

- [ListenerStore](gogurt-listener-runtime-listenerstore.md)

## Governing policies

- <a id="pa-332303be83"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [reference/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerStore.observe`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9c4afd9f110e365bfde5140272698d975c544ce87babc31b559701d2e33d4aab -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, mount_points: 'Sequence[Path]', planner: 'Callable[[Path], Mapping[str, object]]', *, now: 'float') -> 'list[str]'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "observe",
  "owner": "gogurt_listener_runtime.ListenerStore",
  "unit": "member"
}
```

</details>
