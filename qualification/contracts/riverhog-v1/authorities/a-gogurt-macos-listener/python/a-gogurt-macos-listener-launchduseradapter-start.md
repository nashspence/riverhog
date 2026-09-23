# a_gogurt_macos_listener.LaunchdUserAdapter.start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-macos-listener:a-gogurt-macos-listener-launchduseradapter-start:752303bb62 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-macos-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cda4da690e"></a>
- <a id="s-ec06c4eb7d"></a>`distribution`: `a-gogurt-macos-listener`
- <a id="s-9b96f7c01f"></a>`module`: `a_gogurt_macos_listener`
- <a id="s-a65f97c792"></a>`name`: `start`
- <a id="s-4ca28e0990"></a>`owner`: `a_gogurt_macos_listener.LaunchdUserAdapter`
- <a id="s-bfa2847c50"></a>`unit`: `member`

### Declared structure

- <a id="s-bce7b2374d"></a>`kind`: `"method"`
- <a id="s-0ada39f6b1"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [LaunchdUserAdapter](a-gogurt-macos-listener-launchduseradapter.md)

## Governing policies

- <a id="pa-39f0b44afc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-macos-listener:a_gogurt_macos_listener](../../../evidence/sources/authorities.md#src-94f2d0391f) — [some-implementations/gogurt/listener-host/macos/src/a\_gogurt\_macos\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/macos/src/a_gogurt_macos_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_macos_listener.LaunchdUserAdapter.start`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e749bdbf34f422aa50b3d5a035bc49b8a760a5461b020cb1d1fad6f9208043d4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "a-gogurt-macos-listener",
  "module": "a_gogurt_macos_listener",
  "name": "start",
  "owner": "a_gogurt_macos_listener.LaunchdUserAdapter",
  "unit": "member"
}
```

</details>
