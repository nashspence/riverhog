# gogurt_listener_runtime.ListenerStore.create

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerstore-create:13038c5fee -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-445bf8ca46"></a>
- <a id="s-bd80420a37"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-5683b72bf6"></a>`module`: `gogurt_listener_runtime`
- <a id="s-6e37546668"></a>`name`: `create`
- <a id="s-d17a1ad147"></a>`owner`: `gogurt_listener_runtime.ListenerStore`
- <a id="s-8c239e938f"></a>`unit`: `member`

### Declared structure

- <a id="s-0d34e643d7"></a>`kind`: `"method"`
- <a id="s-ebf0cd838f"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ListenerStore](gogurt-listener-runtime-listenerstore.md)

## Governing policies

- <a id="pa-afc30b82f4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [some-implementations/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerStore.create`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5a1cccab1062b77514756effbba5149af5e928f2b7bb2fcc005f892044612869 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "create",
  "owner": "gogurt_listener_runtime.ListenerStore",
  "unit": "member"
}
```

</details>
