# a_riverhog_windows_provenance_contract_lib.CONTRACT_BINDING

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-windows-provenance-contract-lib:a-riverhog-windows-provenance-contract-li-e11c91ea79:a3f755847b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-windows-provenance-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1028e9cd31"></a>
- <a id="s-7a8f6429fe"></a>`distribution`: `a-riverhog-windows-provenance-contract-lib`
- <a id="s-2696668576"></a>`module`: `a_riverhog_windows_provenance_contract_lib`
- <a id="s-628d254368"></a>`name`: `CONTRACT_BINDING`
- <a id="s-f316adb7f8"></a>`unit`: `export`

### Declared structure

- <a id="s-b472010cfc"></a>`kind`: `"object"`
- <a id="s-f64f802b22"></a>`type`: `"riverhog_provenance_contracts.ProvenanceContractBinding"`

## Governing policies

- <a id="pa-991749715a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-windows-provenance-contract-lib:a_riverhog_windows_provenance_contract_lib](../../../evidence/sources/authorities.md#src-85ad7caa5f) — [some-implementations/riverhog/provenance/contracts/windows/src/a\_riverhog\_windows\_provenance\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/provenance/contracts/windows/src/a_riverhog_windows_provenance_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_windows_provenance_contract_lib.CONTRACT_BINDING`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3c3c34c878e519be2eea54760381a452ed0830b77b3baba1b1806f4c2b3d152a -->

```json
{
  "contract": {
    "kind": "object",
    "type": "riverhog_provenance_contracts.ProvenanceContractBinding"
  },
  "distribution": "a-riverhog-windows-provenance-contract-lib",
  "module": "a_riverhog_windows_provenance_contract_lib",
  "name": "CONTRACT_BINDING",
  "unit": "export"
}
```

</details>
