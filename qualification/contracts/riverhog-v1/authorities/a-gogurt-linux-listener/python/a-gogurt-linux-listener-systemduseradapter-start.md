# a_gogurt_linux_listener.SystemdUserAdapter.start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-linux-listener:a-gogurt-linux-listener-systemduseradapter-start:a77362ccfa -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-linux-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ad8971d701"></a>
- <a id="s-4198e71658"></a>`distribution`: `a-gogurt-linux-listener`
- <a id="s-3abb7e1c1f"></a>`module`: `a_gogurt_linux_listener`
- <a id="s-10a370d238"></a>`name`: `start`
- <a id="s-507bcb39ab"></a>`owner`: `a_gogurt_linux_listener.SystemdUserAdapter`
- <a id="s-635c1df1bb"></a>`unit`: `member`

### Declared structure

- <a id="s-ecad1b9dd0"></a>`kind`: `"method"`
- <a id="s-5a16601525"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [SystemdUserAdapter](a-gogurt-linux-listener-systemduseradapter.md)

## Governing policies

- <a id="pa-78b73a4d5e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-linux-listener:a_gogurt_linux_listener](../../../evidence/sources/authorities.md#src-5662842a8e) — [some-implementations/gogurt/listener-host/linux/src/a\_gogurt\_linux\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/linux/src/a_gogurt_linux_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_linux_listener.SystemdUserAdapter.start`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f4e525062481f4634949dc8cf572b3fe23ef81faf816379a9a7b2e65c1a33161 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "a-gogurt-linux-listener",
  "module": "a_gogurt_linux_listener",
  "name": "start",
  "owner": "a_gogurt_linux_listener.SystemdUserAdapter",
  "unit": "member"
}
```

</details>
