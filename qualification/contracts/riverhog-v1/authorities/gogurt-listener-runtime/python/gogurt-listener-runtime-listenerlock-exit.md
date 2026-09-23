# gogurt_listener_runtime.ListenerLock.__exit__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerlock-exit:1da464c105 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5827093274"></a>
- <a id="s-bac54de997"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-75ff4781da"></a>`module`: `gogurt_listener_runtime`
- <a id="s-66dcf4fba0"></a>`name`: `__exit__`
- <a id="s-012d98d3e0"></a>`owner`: `gogurt_listener_runtime.ListenerLock`
- <a id="s-6f6f7229ba"></a>`unit`: `member`

### Declared structure

- <a id="s-05227a71cc"></a>`kind`: `"method"`
- <a id="s-2321ab4114"></a>`signature`: `"\"(self, _type: 'object', _value: 'object', _traceback: 'object') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ListenerLock](gogurt-listener-runtime-listenerlock.md)

## Governing policies

- <a id="pa-dbba211cff"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerLock.__exit__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a58e4717a85f7e6a8bfab76d4ccfc7cda62ab492dec7d95343da1746c3677d7a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, _type: 'object', _value: 'object', _traceback: 'object') -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "__exit__",
  "owner": "gogurt_listener_runtime.ListenerLock",
  "unit": "member"
}
```

</details>
