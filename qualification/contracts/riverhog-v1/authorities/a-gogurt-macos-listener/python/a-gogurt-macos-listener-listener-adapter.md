# a_gogurt_macos_listener.listener_adapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-macos-listener:a-gogurt-macos-listener-listener-adapter:954fb04eae -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-macos-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3bbeb58f4b"></a>
- <a id="s-07c018827f"></a>`distribution`: `a-gogurt-macos-listener`
- <a id="s-573e7e3c99"></a>`module`: `a_gogurt_macos_listener`
- <a id="s-51aedacc4e"></a>`name`: `listener_adapter`
- <a id="s-5a8cd02275"></a>`unit`: `export`

### Declared structure

- <a id="s-c33e47cb9f"></a>`kind`: `"function"`
- <a id="s-2b3958b446"></a>`signature`: `"\"(*, environment: 'Mapping[str, str] \| None' = None, home: 'Path \| None' = None) -> 'ListenerAdapter'\""`

## Governing policies

- <a id="pa-cc3cdbf1e8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-macos-listener:a_gogurt_macos_listener](../../../evidence/sources/authorities.md#src-94f2d0391f) — [some-implementations/gogurt/listener-host/macos/src/a\_gogurt\_macos\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/macos/src/a_gogurt_macos_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_macos_listener.listener_adapter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ed226337863fd637fb60fe275b8ce00cf1ca366fe8e2d58ceb46bfbd98eb943 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, environment: 'Mapping[str, str] | None' = None, home: 'Path | None' = None) -> 'ListenerAdapter'\""
  },
  "distribution": "a-gogurt-macos-listener",
  "module": "a_gogurt_macos_listener",
  "name": "listener_adapter",
  "unit": "export"
}
```

</details>
