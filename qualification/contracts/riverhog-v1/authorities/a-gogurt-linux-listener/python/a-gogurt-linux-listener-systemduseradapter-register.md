# a_gogurt_linux_listener.SystemdUserAdapter.register

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-linux-listener:a-gogurt-linux-listener-systemduseradapter-register:5d09a2e4bb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-linux-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fedf72f6e0"></a>
- <a id="s-e82ad79708"></a>`distribution`: `a-gogurt-linux-listener`
- <a id="s-3911722f54"></a>`module`: `a_gogurt_linux_listener`
- <a id="s-358fe4b6bb"></a>`name`: `register`
- <a id="s-416ec290f8"></a>`owner`: `a_gogurt_linux_listener.SystemdUserAdapter`
- <a id="s-6ee8cf1cb9"></a>`unit`: `member`

### Declared structure

- <a id="s-de2eacbe57"></a>`kind`: `"method"`
- <a id="s-321474ecdd"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths', command: 'Sequence[str]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [SystemdUserAdapter](a-gogurt-linux-listener-systemduseradapter.md)

## Governing policies

- <a id="pa-b71f29465e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-linux-listener:a_gogurt_linux_listener](../../../evidence/sources/authorities.md#src-5662842a8e) — [some-implementations/gogurt/listener-host/linux/src/a\_gogurt\_linux\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/linux/src/a_gogurt_linux_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_linux_listener.SystemdUserAdapter.register`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6bb58c9d21745ba46de49d481e44fad0845eeb9071ebcfc6d68f7b5726a5283e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths', command: 'Sequence[str]') -> 'None'\""
  },
  "distribution": "a-gogurt-linux-listener",
  "module": "a_gogurt_linux_listener",
  "name": "register",
  "owner": "a_gogurt_linux_listener.SystemdUserAdapter",
  "unit": "member"
}
```

</details>
