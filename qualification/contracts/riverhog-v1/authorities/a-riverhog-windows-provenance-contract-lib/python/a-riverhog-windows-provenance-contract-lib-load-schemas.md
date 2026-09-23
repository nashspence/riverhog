# a_riverhog_windows_provenance_contract_lib.load_schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-windows-provenance-contract-lib:a-riverhog-windows-provenance-contract-li-e08cfcc234:f578590c34 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-windows-provenance-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3910f5144b"></a>
- <a id="s-ee3a548fc4"></a>`distribution`: `a-riverhog-windows-provenance-contract-lib`
- <a id="s-b3d73603c9"></a>`module`: `a_riverhog_windows_provenance_contract_lib`
- <a id="s-ba2dbed4d8"></a>`name`: `load_schemas`
- <a id="s-05e23bf5d4"></a>`unit`: `export`

### Declared structure

- <a id="s-b8d464b594"></a>`kind`: `"function"`
- <a id="s-aa721e1241"></a>`signature`: `"\"() -> 'dict[str, dict[str, Any]]'\""`

## Governing policies

- <a id="pa-88840b1b35"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-windows-provenance-contract-lib:a_riverhog_windows_provenance_contract_lib](../../../evidence/sources/authorities.md#src-85ad7caa5f) — [some-implementations/riverhog/provenance/contracts/windows/src/a\_riverhog\_windows\_provenance\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/provenance/contracts/windows/src/a_riverhog_windows_provenance_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_windows_provenance_contract_lib.load_schemas`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 05a2ebd915e6cd9f11b8add8686003c9001a3b87d2525403429807111e87dc62 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'dict[str, dict[str, Any]]'\""
  },
  "distribution": "a-riverhog-windows-provenance-contract-lib",
  "module": "a_riverhog_windows_provenance_contract_lib",
  "name": "load_schemas",
  "unit": "export"
}
```

</details>
