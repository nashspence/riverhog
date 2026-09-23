# a_riverhog_macos_provenance_contract_lib.CONTRACT_BINDING

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-macos-provenance-contract-lib:a-riverhog-macos-provenance-contract-lib-e73a75a8e2:dc3f9bf906 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-macos-provenance-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-34221dc54f"></a>
- <a id="s-f17d352c4e"></a>`distribution`: `a-riverhog-macos-provenance-contract-lib`
- <a id="s-429d1b40e8"></a>`module`: `a_riverhog_macos_provenance_contract_lib`
- <a id="s-04728718bb"></a>`name`: `CONTRACT_BINDING`
- <a id="s-897f8abcb2"></a>`unit`: `export`

### Declared structure

- <a id="s-4fe8a3e7d1"></a>`kind`: `"object"`
- <a id="s-3789980787"></a>`type`: `"riverhog_provenance_contracts.ProvenanceContractBinding"`

## Governing policies

- <a id="pa-9eddb38f3b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-macos-provenance-contract-lib:a_riverhog_macos_provenance_contract_lib](../../../evidence/sources/authorities.md#src-273ca071fb) — [some-implementations/riverhog/provenance/contracts/macos/src/a\_riverhog\_macos\_provenance\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/provenance/contracts/macos/src/a_riverhog_macos_provenance_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_macos_provenance_contract_lib.CONTRACT_BINDING`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ffbbc708af691cfb792b8d41d5e6dad725fac43e7b17e08af8710c6f5af93bb1 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "riverhog_provenance_contracts.ProvenanceContractBinding"
  },
  "distribution": "a-riverhog-macos-provenance-contract-lib",
  "module": "a_riverhog_macos_provenance_contract_lib",
  "name": "CONTRACT_BINDING",
  "unit": "export"
}
```

</details>
