# a_gogurt_windows_listener.WINDOWS_TASK_RESTART_INTERVAL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-gogurt-windows-listener:a-gogurt-windows-listener-windows-task-re-246911cff6:bf0aa97fc6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3d28d52bfd"></a>
- <a id="s-c0b5777966"></a>`distribution`: `a-gogurt-windows-listener`
- <a id="s-176526c9a9"></a>`module`: `a_gogurt_windows_listener`
- <a id="s-8d4bcb87ee"></a>`name`: `WINDOWS_TASK_RESTART_INTERVAL`
- <a id="s-d31f0c6e15"></a>`unit`: `export`

### Declared structure

- <a id="s-6ffc1c0d23"></a>`kind`: `"constant"`
- <a id="s-5cdd0ac2d6"></a>`value`: `"PT1M"`

## Governing policies

- <a id="pa-f556e63eea"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-gogurt-windows-listener:a_gogurt_windows_listener](../../../evidence/sources/authorities.md#src-29f22753ba) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)

### Machine authority

- `/external_contract/python/a_gogurt_windows_listener.WINDOWS_TASK_RESTART_INTERVAL`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b923757f462ae4b2e93489c741ba64134912ba474b2365562a0c856cdf09d6b -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "PT1M"
  },
  "distribution": "a-gogurt-windows-listener",
  "module": "a_gogurt_windows_listener",
  "name": "WINDOWS_TASK_RESTART_INTERVAL",
  "unit": "export"
}
```

</details>
