# gogurt_listener_runtime.ListenerAdapter.register

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listeneradapter-register:b4e09de2a8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-220d399221"></a>
- <a id="s-52ced17516"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-5598887821"></a>`module`: `gogurt_listener_runtime`
- <a id="s-3feabfc720"></a>`name`: `register`
- <a id="s-68ccd045a9"></a>`owner`: `gogurt_listener_runtime.ListenerAdapter`
- <a id="s-225639bdcd"></a>`unit`: `member`

### Declared structure

- <a id="s-0c542a1585"></a>`kind`: `"method"`
- <a id="s-e99f0283ff"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths', command: 'Sequence[str]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ListenerAdapter](gogurt-listener-runtime-listeneradapter.md)

## Governing policies

- <a id="pa-0538a9e7ee"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerAdapter.register`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e4844b9fb551f96ae2bb7d035dd2dad7e52da753810ac99eaebd7c71df85c5ae -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths', command: 'Sequence[str]') -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "register",
  "owner": "gogurt_listener_runtime.ListenerAdapter",
  "unit": "member"
}
```

</details>
