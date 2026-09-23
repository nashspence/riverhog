# gogurt_listener_runtime.ListenerConfig.read

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerconfig-read:ecaf14ded9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-093eacc82d"></a>
- <a id="s-ff3d420a0e"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-b69b7ec7f9"></a>`module`: `gogurt_listener_runtime`
- <a id="s-a74dc5e95b"></a>`name`: `read`
- <a id="s-1cc70af083"></a>`owner`: `gogurt_listener_runtime.ListenerConfig`
- <a id="s-c6caa26d81"></a>`unit`: `member`

### Declared structure

- <a id="s-8e7d550f0b"></a>`kind`: `"classmethod"`
- <a id="s-d263e2cac2"></a>`signature`: `"\"(cls, path: 'Path') -> 'ListenerConfig'\""`

## Maintained corroboration

### Related interface records

- [ListenerConfig](gogurt-listener-runtime-listenerconfig.md)

## Governing policies

- <a id="pa-58cb450dfb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerConfig.read`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9d4c978754aed240337c4f5ce0801239df736dce9e11c56a43b33ef10bdb9da0 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, path: 'Path') -> 'ListenerConfig'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "read",
  "owner": "gogurt_listener_runtime.ListenerConfig",
  "unit": "member"
}
```

</details>
