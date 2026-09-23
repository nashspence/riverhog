# a_riverhog_windows_provenance_contract_lib.CONTRACT_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-windows-provenance-contract-lib:a-riverhog-windows-provenance-contract-li-3b47704843:bb62d4d758 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-windows-provenance-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-25d60dabb7"></a>
- <a id="s-edb9587c54"></a>`distribution`: `a-riverhog-windows-provenance-contract-lib`
- <a id="s-4b2f5ceab5"></a>`module`: `a_riverhog_windows_provenance_contract_lib`
- <a id="s-5a956105dd"></a>`name`: `CONTRACT_ID`
- <a id="s-0deb8f50c2"></a>`unit`: `export`

### Declared structure

- <a id="s-9abf370a1c"></a>`kind`: `"constant"`
- <a id="s-8ac482d2fa"></a>`value`: `"riverhog-provenance-windows-observation/v1"`

## Governing policies

- <a id="pa-4923e3f5ca"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-windows-provenance-contract-lib:a_riverhog_windows_provenance_contract_lib](../../../evidence/sources/authorities.md#src-85ad7caa5f) — [some-implementations/riverhog/provenance/contracts/windows/src/a\_riverhog\_windows\_provenance\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/provenance/contracts/windows/src/a_riverhog_windows_provenance_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_windows_provenance_contract_lib.CONTRACT_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c458831d8938721c6d47d646070c1ef66958d5fc346d7a19e1a72c4e6a6f4a4 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-provenance-windows-observation/v1"
  },
  "distribution": "a-riverhog-windows-provenance-contract-lib",
  "module": "a_riverhog_windows_provenance_contract_lib",
  "name": "CONTRACT_ID",
  "unit": "export"
}
```

</details>
