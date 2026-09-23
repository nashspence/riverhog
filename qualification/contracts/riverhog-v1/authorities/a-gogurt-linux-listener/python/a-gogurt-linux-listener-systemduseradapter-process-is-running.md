# a_gogurt_linux_listener.SystemdUserAdapter.process_is_running

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-linux-listener:a-gogurt-linux-listener-systemduseradapte-933d8a8a1d:a6dfa23ecf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-linux-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3bde28c0d3"></a>
- <a id="s-2d396150e7"></a>`distribution`: `a-gogurt-linux-listener`
- <a id="s-766a97b0da"></a>`module`: `a_gogurt_linux_listener`
- <a id="s-f597bbf6c3"></a>`name`: `process_is_running`
- <a id="s-34071ed0d8"></a>`owner`: `a_gogurt_linux_listener.SystemdUserAdapter`
- <a id="s-1678417bcf"></a>`unit`: `member`

### Declared structure

- <a id="s-360255a737"></a>`kind`: `"staticmethod"`
- <a id="s-e2fc53a45c"></a>`signature`: `"\"(pid: 'int') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [SystemdUserAdapter](a-gogurt-linux-listener-systemduseradapter.md)

## Governing policies

- <a id="pa-634a004a25"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-linux-listener:a_gogurt_linux_listener](../../../evidence/sources/authorities.md#src-5662842a8e) — [some-implementations/gogurt/listener-host/linux/src/a\_gogurt\_linux\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/linux/src/a_gogurt_linux_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_linux_listener.SystemdUserAdapter.process_is_running`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: afd6e082aec100bb13701a5791b3d266986e29753d0be35af4b38a35f1cf370c -->

```json
{
  "contract": {
    "kind": "staticmethod",
    "signature": "\"(pid: 'int') -> 'bool'\""
  },
  "distribution": "a-gogurt-linux-listener",
  "module": "a_gogurt_linux_listener",
  "name": "process_is_running",
  "owner": "a_gogurt_linux_listener.SystemdUserAdapter",
  "unit": "member"
}
```

</details>
