# a_gogurt_macos_listener.LaunchdUserAdapter.register

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-macos-listener:a-gogurt-macos-listener-launchduseradapter-register:75e49f1b19 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-macos-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5190795bcb"></a>
- <a id="s-b0277c005b"></a>`distribution`: `a-gogurt-macos-listener`
- <a id="s-9f213fbb41"></a>`module`: `a_gogurt_macos_listener`
- <a id="s-b2c75d8441"></a>`name`: `register`
- <a id="s-41b71030e6"></a>`owner`: `a_gogurt_macos_listener.LaunchdUserAdapter`
- <a id="s-c9b395120e"></a>`unit`: `member`

### Declared structure

- <a id="s-ae714c2e35"></a>`kind`: `"method"`
- <a id="s-b7da74acf0"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths', command: 'Sequence[str]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [LaunchdUserAdapter](a-gogurt-macos-listener-launchduseradapter.md)

## Governing policies

- <a id="pa-3117593d17"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-macos-listener:a_gogurt_macos_listener](../../../evidence/sources/authorities.md#src-94f2d0391f) — [some-implementations/gogurt/listener-host/macos/src/a\_gogurt\_macos\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/macos/src/a_gogurt_macos_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_macos_listener.LaunchdUserAdapter.register`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 15f507b0218efaa694ec2ed60904a05130e8cd996e0dd77d95fbb206e8c6fd48 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths', command: 'Sequence[str]') -> 'None'\""
  },
  "distribution": "a-gogurt-macos-listener",
  "module": "a_gogurt_macos_listener",
  "name": "register",
  "owner": "a_gogurt_macos_listener.LaunchdUserAdapter",
  "unit": "member"
}
```

</details>
