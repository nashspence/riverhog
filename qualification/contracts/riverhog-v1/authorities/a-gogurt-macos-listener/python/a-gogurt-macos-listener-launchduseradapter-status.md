# a_gogurt_macos_listener.LaunchdUserAdapter.status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-macos-listener:a-gogurt-macos-listener-launchduseradapter-status:16297ba140 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-macos-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7b57e01007"></a>
- <a id="s-44165abe58"></a>`distribution`: `a-gogurt-macos-listener`
- <a id="s-d3201e7fec"></a>`module`: `a_gogurt_macos_listener`
- <a id="s-da0e27d40f"></a>`name`: `status`
- <a id="s-1c1d4efc2f"></a>`owner`: `a_gogurt_macos_listener.LaunchdUserAdapter`
- <a id="s-47b3f8f362"></a>`unit`: `member`

### Declared structure

- <a id="s-841dd5e824"></a>`kind`: `"method"`
- <a id="s-2ce00a09c3"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'NativeListenerStatus'\""`

## Maintained corroboration

### Related interface records

- [LaunchdUserAdapter](a-gogurt-macos-listener-launchduseradapter.md)

## Governing policies

- <a id="pa-b3b847da03"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-macos-listener:a_gogurt_macos_listener](../../../evidence/sources/authorities.md#src-94f2d0391f) — [some-implementations/gogurt/listener-host/macos/src/a\_gogurt\_macos\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/macos/src/a_gogurt_macos_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_macos_listener.LaunchdUserAdapter.status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bf6470222ca245b020a9eca2d2a64e4057120fcf41db3abed13222a29f468aef -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'NativeListenerStatus'\""
  },
  "distribution": "a-gogurt-macos-listener",
  "module": "a_gogurt_macos_listener",
  "name": "status",
  "owner": "a_gogurt_macos_listener.LaunchdUserAdapter",
  "unit": "member"
}
```

</details>
