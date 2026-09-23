# a_gogurt_macos_listener.LaunchdUserAdapter.process_is_running

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-macos-listener:a-gogurt-macos-listener-launchduseradapte-109656894f:5092dc2738 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-macos-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3cf67a3b5c"></a>
- <a id="s-3fbe0f7b4d"></a>`distribution`: `a-gogurt-macos-listener`
- <a id="s-ef05dcc6de"></a>`module`: `a_gogurt_macos_listener`
- <a id="s-0a02fbac55"></a>`name`: `process_is_running`
- <a id="s-fc83c088b0"></a>`owner`: `a_gogurt_macos_listener.LaunchdUserAdapter`
- <a id="s-41989ce0f9"></a>`unit`: `member`

### Declared structure

- <a id="s-0768a22109"></a>`kind`: `"staticmethod"`
- <a id="s-c4895e1801"></a>`signature`: `"\"(pid: 'int') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [LaunchdUserAdapter](a-gogurt-macos-listener-launchduseradapter.md)

## Governing policies

- <a id="pa-61a6443b4d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-macos-listener:a_gogurt_macos_listener](../../../evidence/sources/authorities.md#src-94f2d0391f) — [some-implementations/gogurt/listener-host/macos/src/a\_gogurt\_macos\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/macos/src/a_gogurt_macos_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_macos_listener.LaunchdUserAdapter.process_is_running`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 863b27d9a31ae24fb6451d6ee02d8fb9fba4120decd2f05a5badc8389d2d76ca -->

```json
{
  "contract": {
    "kind": "staticmethod",
    "signature": "\"(pid: 'int') -> 'bool'\""
  },
  "distribution": "a-gogurt-macos-listener",
  "module": "a_gogurt_macos_listener",
  "name": "process_is_running",
  "owner": "a_gogurt_macos_listener.LaunchdUserAdapter",
  "unit": "member"
}
```

</details>
