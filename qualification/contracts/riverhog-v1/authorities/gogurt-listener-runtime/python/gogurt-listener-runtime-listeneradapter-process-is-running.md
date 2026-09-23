# gogurt_listener_runtime.ListenerAdapter.process_is_running

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listeneradapter-p-a015db742d:0de9cf095f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a4e648193f"></a>
- <a id="s-0f6dfd77f1"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-332c8fef54"></a>`module`: `gogurt_listener_runtime`
- <a id="s-e607e784c1"></a>`name`: `process_is_running`
- <a id="s-f731b91355"></a>`owner`: `gogurt_listener_runtime.ListenerAdapter`
- <a id="s-2f483ccadf"></a>`unit`: `member`

### Declared structure

- <a id="s-7092be9c44"></a>`kind`: `"method"`
- <a id="s-65c01bdadd"></a>`signature`: `"\"(self, pid: 'int') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [ListenerAdapter](gogurt-listener-runtime-listeneradapter.md)

## Governing policies

- <a id="pa-11b1c52e01"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerAdapter.process_is_running`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 78716486789e0824b736556b30e44d99e44e68bf0d788ffdc63154ec1d143bd6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, pid: 'int') -> 'bool'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "process_is_running",
  "owner": "gogurt_listener_runtime.ListenerAdapter",
  "unit": "member"
}
```

</details>
