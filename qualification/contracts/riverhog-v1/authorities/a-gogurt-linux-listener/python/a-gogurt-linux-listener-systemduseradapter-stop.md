# a_gogurt_linux_listener.SystemdUserAdapter.stop

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-linux-listener:a-gogurt-linux-listener-systemduseradapter-stop:f649d0a9cc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-linux-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-61cfbfa986"></a>
- <a id="s-32c274bea3"></a>`distribution`: `a-gogurt-linux-listener`
- <a id="s-086713523a"></a>`module`: `a_gogurt_linux_listener`
- <a id="s-7aaaabb97e"></a>`name`: `stop`
- <a id="s-c565861178"></a>`owner`: `a_gogurt_linux_listener.SystemdUserAdapter`
- <a id="s-4a21f2c000"></a>`unit`: `member`

### Declared structure

- <a id="s-705e9a32cf"></a>`kind`: `"method"`
- <a id="s-83f221bcff"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [SystemdUserAdapter](a-gogurt-linux-listener-systemduseradapter.md)

## Governing policies

- <a id="pa-cd6717669d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-linux-listener:a_gogurt_linux_listener](../../../evidence/sources/authorities.md#src-5662842a8e) — [some-implementations/gogurt/listener-host/linux/src/a\_gogurt\_linux\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/linux/src/a_gogurt_linux_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_linux_listener.SystemdUserAdapter.stop`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 165e04302845d0ab4303bfe09b88e28325c5400d0b3a8ec887a7c0d12dc07a2f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "a-gogurt-linux-listener",
  "module": "a_gogurt_linux_listener",
  "name": "stop",
  "owner": "a_gogurt_linux_listener.SystemdUserAdapter",
  "unit": "member"
}
```

</details>
