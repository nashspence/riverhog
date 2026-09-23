# a_gogurt_macos_listener.LaunchdUserAdapter.stop

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-macos-listener:a-gogurt-macos-listener-launchduseradapter-stop:3b083bff71 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-macos-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-04207b785e"></a>
- <a id="s-13da2a9459"></a>`distribution`: `a-gogurt-macos-listener`
- <a id="s-fa45f56f24"></a>`module`: `a_gogurt_macos_listener`
- <a id="s-6f242b37e6"></a>`name`: `stop`
- <a id="s-c8bae59722"></a>`owner`: `a_gogurt_macos_listener.LaunchdUserAdapter`
- <a id="s-d2cb2bf961"></a>`unit`: `member`

### Declared structure

- <a id="s-375cf1dfa6"></a>`kind`: `"method"`
- <a id="s-3ad070f961"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [LaunchdUserAdapter](a-gogurt-macos-listener-launchduseradapter.md)

## Governing policies

- <a id="pa-e24f325114"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-macos-listener:a_gogurt_macos_listener](../../../evidence/sources/authorities.md#src-94f2d0391f) — [some-implementations/gogurt/listener-host/macos/src/a\_gogurt\_macos\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/macos/src/a_gogurt_macos_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_macos_listener.LaunchdUserAdapter.stop`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0b4ae483fd768e2534fd81254f78fd748d8d7dfa5e08241482b44b7d8b87294e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "a-gogurt-macos-listener",
  "module": "a_gogurt_macos_listener",
  "name": "stop",
  "owner": "a_gogurt_macos_listener.LaunchdUserAdapter",
  "unit": "member"
}
```

</details>
