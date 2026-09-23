# gogurt_listener_runtime.ListenerRuntime.run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerruntime-run:55f4021fb9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4a85613d33"></a>
- <a id="s-ea832f2df4"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-60deddb60d"></a>`module`: `gogurt_listener_runtime`
- <a id="s-90a453470f"></a>`name`: `run`
- <a id="s-f10805cbe4"></a>`owner`: `gogurt_listener_runtime.ListenerRuntime`
- <a id="s-ee136ad1db"></a>`unit`: `member`

### Declared structure

- <a id="s-0f4c1facf3"></a>`kind`: `"method"`
- <a id="s-5726d8225c"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ListenerRuntime](gogurt-listener-runtime-listenerruntime.md)

## Governing policies

- <a id="pa-d7e8054b90"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerRuntime.run`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0dcdf94519975d41b8cc3654302a720b74ba17ba085fb611ea665a2454c8e3d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "run",
  "owner": "gogurt_listener_runtime.ListenerRuntime",
  "unit": "member"
}
```

</details>
