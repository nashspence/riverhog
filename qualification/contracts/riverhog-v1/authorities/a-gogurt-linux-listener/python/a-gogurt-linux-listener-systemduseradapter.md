# a_gogurt_linux_listener.SystemdUserAdapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-linux-listener:a-gogurt-linux-listener-systemduseradapter:556bd28ce7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-linux-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c51e6369b5"></a>
- <a id="s-af71281620"></a>`distribution`: `a-gogurt-linux-listener`
- <a id="s-28632dd761"></a>`module`: `a_gogurt_linux_listener`
- <a id="s-5a56dabfee"></a>`name`: `SystemdUserAdapter`
- <a id="s-6b40310d1c"></a>`unit`: `export`

### Declared structure

- <a id="s-8acbda2ded"></a>`kind`: `"class"`
- <a id="s-31a7e59a0e"></a>`signature`: `"\"(registration_file: 'Path') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [unregister](a-gogurt-linux-listener-systemduseradapter-unregister.md)
- [process_is_running](a-gogurt-linux-listener-systemduseradapter-process-is-running.md)
- [register](a-gogurt-linux-listener-systemduseradapter-register.md)
- [start](a-gogurt-linux-listener-systemduseradapter-start.md)
- [status](a-gogurt-linux-listener-systemduseradapter-status.md)
- [stop](a-gogurt-linux-listener-systemduseradapter-stop.md)

## Governing policies

- <a id="pa-ee60ad8e8a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-linux-listener:a_gogurt_linux_listener](../../../evidence/sources/authorities.md#src-5662842a8e) — [some-implementations/gogurt/listener-host/linux/src/a\_gogurt\_linux\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/linux/src/a_gogurt_linux_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_linux_listener.SystemdUserAdapter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0bd825dab917064435c8523f7226baa3fccdfe21cf2bb5283b08a48149726a46 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(registration_file: 'Path') -> 'None'\""
  },
  "distribution": "a-gogurt-linux-listener",
  "module": "a_gogurt_linux_listener",
  "name": "SystemdUserAdapter",
  "unit": "export"
}
```

</details>
