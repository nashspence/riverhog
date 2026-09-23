# a_riverhog_macos_provenance_contract_lib.CONTRACT_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-macos-provenance-contract-lib:a-riverhog-macos-provenance-contract-lib-contract-id:0809424b15 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-macos-provenance-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a8cf86a0b8"></a>
- <a id="s-1347c8e69f"></a>`distribution`: `a-riverhog-macos-provenance-contract-lib`
- <a id="s-0684594df2"></a>`module`: `a_riverhog_macos_provenance_contract_lib`
- <a id="s-9dcc0e7326"></a>`name`: `CONTRACT_ID`
- <a id="s-d9fdf3fcaa"></a>`unit`: `export`

### Declared structure

- <a id="s-fcba0e8052"></a>`kind`: `"constant"`
- <a id="s-4eac424246"></a>`value`: `"riverhog-provenance-macos-observation/v1"`

## Governing policies

- <a id="pa-1c4f56a11e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-macos-provenance-contract-lib:a_riverhog_macos_provenance_contract_lib](../../../evidence/sources/authorities.md#src-273ca071fb) — [some-implementations/riverhog/provenance/contracts/macos/src/a\_riverhog\_macos\_provenance\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/provenance/contracts/macos/src/a_riverhog_macos_provenance_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_macos_provenance_contract_lib.CONTRACT_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 148e24f5cc3410e00e8ce2c7d02a95015be0f8c8ca6bd837d2f2fd603555bb31 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-provenance-macos-observation/v1"
  },
  "distribution": "a-riverhog-macos-provenance-contract-lib",
  "module": "a_riverhog_macos_provenance_contract_lib",
  "name": "CONTRACT_ID",
  "unit": "export"
}
```

</details>
