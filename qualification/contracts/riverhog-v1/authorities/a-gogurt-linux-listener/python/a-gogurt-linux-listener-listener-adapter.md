# a_gogurt_linux_listener.listener_adapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-linux-listener:a-gogurt-linux-listener-listener-adapter:5b039c8542 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-linux-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d2e461cbc0"></a>
- <a id="s-54f652728a"></a>`distribution`: `a-gogurt-linux-listener`
- <a id="s-df358c0741"></a>`module`: `a_gogurt_linux_listener`
- <a id="s-46ce4182b6"></a>`name`: `listener_adapter`
- <a id="s-70dbc5e519"></a>`unit`: `export`

### Declared structure

- <a id="s-a05ada4b69"></a>`kind`: `"function"`
- <a id="s-011453d10d"></a>`signature`: `"\"(*, environment: 'Mapping[str, str] \| None' = None, home: 'Path \| None' = None) -> 'ListenerAdapter'\""`

## Governing policies

- <a id="pa-78b3eea78d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-linux-listener:a_gogurt_linux_listener](../../../evidence/sources/authorities.md#src-5662842a8e) — [some-implementations/gogurt/listener-host/linux/src/a\_gogurt\_linux\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/linux/src/a_gogurt_linux_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_linux_listener.listener_adapter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dcf8601a7897553321516aeac02f2f6d2339eea0d86b43b21af39aec6c6aff8a -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, environment: 'Mapping[str, str] | None' = None, home: 'Path | None' = None) -> 'ListenerAdapter'\""
  },
  "distribution": "a-gogurt-linux-listener",
  "module": "a_gogurt_linux_listener",
  "name": "listener_adapter",
  "unit": "export"
}
```

</details>
