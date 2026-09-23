# a_gogurt_linux_listener.SystemdUserAdapter.unregister

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-linux-listener:a-gogurt-linux-listener-systemduseradapte-3bb64a5ccf:a11b43aaf0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-linux-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6ab8d308c8"></a>
- <a id="s-be92f1d04e"></a>`distribution`: `a-gogurt-linux-listener`
- <a id="s-11b68dd760"></a>`module`: `a_gogurt_linux_listener`
- <a id="s-006fc56fbc"></a>`name`: `unregister`
- <a id="s-5ca1514f38"></a>`owner`: `a_gogurt_linux_listener.SystemdUserAdapter`
- <a id="s-bb1a65af1e"></a>`unit`: `member`

### Declared structure

- <a id="s-e574f73748"></a>`kind`: `"method"`
- <a id="s-003d784bd1"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [SystemdUserAdapter](a-gogurt-linux-listener-systemduseradapter.md)

## Governing policies

- <a id="pa-b25f03e37c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-linux-listener:a_gogurt_linux_listener](../../../evidence/sources/authorities.md#src-5662842a8e) — [some-implementations/gogurt/listener-host/linux/src/a\_gogurt\_linux\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/linux/src/a_gogurt_linux_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_linux_listener.SystemdUserAdapter.unregister`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b422f8e7c11ff13e9a7d0e925f0bae607ec01cb5da266d939eaceb604638ec07 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "a-gogurt-linux-listener",
  "module": "a_gogurt_linux_listener",
  "name": "unregister",
  "owner": "a_gogurt_linux_listener.SystemdUserAdapter",
  "unit": "member"
}
```

</details>
