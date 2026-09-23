# a_gogurt_macos_listener.LaunchdUserAdapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-macos-listener:a-gogurt-macos-listener-launchduseradapter:95fb618618 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-macos-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b24ac24404"></a>
- <a id="s-c921ff5142"></a>`distribution`: `a-gogurt-macos-listener`
- <a id="s-1b514ccd9b"></a>`module`: `a_gogurt_macos_listener`
- <a id="s-ca28f7710b"></a>`name`: `LaunchdUserAdapter`
- <a id="s-500278b1cd"></a>`unit`: `export`

### Declared structure

- <a id="s-39569fa161"></a>`kind`: `"class"`
- <a id="s-a1a630b7a1"></a>`signature`: `"\"(registration_file: 'Path') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [process_is_running](a-gogurt-macos-listener-launchduseradapter-process-is-running.md)
- [unregister](a-gogurt-macos-listener-launchduseradapter-unregister.md)
- [register](a-gogurt-macos-listener-launchduseradapter-register.md)
- [start](a-gogurt-macos-listener-launchduseradapter-start.md)
- [status](a-gogurt-macos-listener-launchduseradapter-status.md)
- [stop](a-gogurt-macos-listener-launchduseradapter-stop.md)

## Governing policies

- <a id="pa-7a0aeeccd4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-macos-listener:a_gogurt_macos_listener](../../../evidence/sources/authorities.md#src-94f2d0391f) — [some-implementations/gogurt/listener-host/macos/src/a\_gogurt\_macos\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/macos/src/a_gogurt_macos_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_macos_listener.LaunchdUserAdapter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd48683bfad7e45cebfcaf2ce4a59b518860a90ec2e6e167ba1b43fe41d0fd9d -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(registration_file: 'Path') -> 'None'\""
  },
  "distribution": "a-gogurt-macos-listener",
  "module": "a_gogurt_macos_listener",
  "name": "LaunchdUserAdapter",
  "unit": "export"
}
```

</details>
