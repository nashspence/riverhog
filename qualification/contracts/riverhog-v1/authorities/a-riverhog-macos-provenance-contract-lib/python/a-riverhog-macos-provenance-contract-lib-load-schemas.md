# a_riverhog_macos_provenance_contract_lib.load_schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-macos-provenance-contract-lib:a-riverhog-macos-provenance-contract-lib-e40b3a10b2:120da04e02 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-macos-provenance-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8f88ec835e"></a>
- <a id="s-114491220a"></a>`distribution`: `a-riverhog-macos-provenance-contract-lib`
- <a id="s-c02519821d"></a>`module`: `a_riverhog_macos_provenance_contract_lib`
- <a id="s-7420cd055f"></a>`name`: `load_schemas`
- <a id="s-68f311d609"></a>`unit`: `export`

### Declared structure

- <a id="s-0869622fb1"></a>`kind`: `"function"`
- <a id="s-77e8bc8400"></a>`signature`: `"\"() -> 'dict[str, dict[str, Any]]'\""`

## Governing policies

- <a id="pa-dd6c10c0d7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-macos-provenance-contract-lib:a_riverhog_macos_provenance_contract_lib](../../../evidence/sources/authorities.md#src-273ca071fb) — [some-implementations/riverhog/provenance/contracts/macos/src/a\_riverhog\_macos\_provenance\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/provenance/contracts/macos/src/a_riverhog_macos_provenance_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_macos_provenance_contract_lib.load_schemas`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 877cbf539f7b8686e85b0a641711fde0c799ca23eb3813cffd9df0e5489b522c -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'dict[str, dict[str, Any]]'\""
  },
  "distribution": "a-riverhog-macos-provenance-contract-lib",
  "module": "a_riverhog_macos_provenance_contract_lib",
  "name": "load_schemas",
  "unit": "export"
}
```

</details>
