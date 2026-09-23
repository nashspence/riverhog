# a_gogurt_linux_listener.SystemdUserAdapter.status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-linux-listener:a-gogurt-linux-listener-systemduseradapter-status:cbb5f0f47c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-linux-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0d97069d28"></a>
- <a id="s-d6d9fa6fa5"></a>`distribution`: `a-gogurt-linux-listener`
- <a id="s-1699a44174"></a>`module`: `a_gogurt_linux_listener`
- <a id="s-99cd8fa3a7"></a>`name`: `status`
- <a id="s-9cc0524947"></a>`owner`: `a_gogurt_linux_listener.SystemdUserAdapter`
- <a id="s-1e70637c3d"></a>`unit`: `member`

### Declared structure

- <a id="s-ae6b8bad47"></a>`kind`: `"method"`
- <a id="s-328d0d9cdc"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'NativeListenerStatus'\""`

## Maintained corroboration

### Related interface records

- [SystemdUserAdapter](a-gogurt-linux-listener-systemduseradapter.md)

## Governing policies

- <a id="pa-a938f9f700"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-linux-listener:a_gogurt_linux_listener](../../../evidence/sources/authorities.md#src-5662842a8e) — [some-implementations/gogurt/listener-host/linux/src/a\_gogurt\_linux\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/linux/src/a_gogurt_linux_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_linux_listener.SystemdUserAdapter.status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b3de2975ca4e69d1412718f1a5325bbb4f7b37f4032de06d4b6b212837d08fbc -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'NativeListenerStatus'\""
  },
  "distribution": "a-gogurt-linux-listener",
  "module": "a_gogurt_linux_listener",
  "name": "status",
  "owner": "a_gogurt_linux_listener.SystemdUserAdapter",
  "unit": "member"
}
```

</details>
