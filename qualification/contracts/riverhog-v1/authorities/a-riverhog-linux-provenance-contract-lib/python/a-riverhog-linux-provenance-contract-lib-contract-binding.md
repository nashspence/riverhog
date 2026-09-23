# a_riverhog_linux_provenance_contract_lib.CONTRACT_BINDING

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-linux-provenance-contract-lib:a-riverhog-linux-provenance-contract-lib-1d2b9f2723:ee2d9165b8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-linux-provenance-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fe77222da2"></a>
- <a id="s-1d94dfa2e1"></a>`distribution`: `a-riverhog-linux-provenance-contract-lib`
- <a id="s-aea7e91270"></a>`module`: `a_riverhog_linux_provenance_contract_lib`
- <a id="s-1555b30cb6"></a>`name`: `CONTRACT_BINDING`
- <a id="s-8cc53e7da9"></a>`unit`: `export`

### Declared structure

- <a id="s-e0cafca344"></a>`kind`: `"object"`
- <a id="s-b04fcbfd67"></a>`type`: `"riverhog_provenance_contracts.ProvenanceContractBinding"`

## Governing policies

- <a id="pa-e2bedd8f71"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-linux-provenance-contract-lib:a_riverhog_linux_provenance_contract_lib](../../../evidence/sources/authorities.md#src-0abeb2023e) — [some-implementations/riverhog/provenance/contracts/linux/src/a\_riverhog\_linux\_provenance\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/provenance/contracts/linux/src/a_riverhog_linux_provenance_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_linux_provenance_contract_lib.CONTRACT_BINDING`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8451805a22c813c8ba7fd630976f0d7e934718f9edeb83233960ad6dbbe05d9e -->

```json
{
  "contract": {
    "kind": "object",
    "type": "riverhog_provenance_contracts.ProvenanceContractBinding"
  },
  "distribution": "a-riverhog-linux-provenance-contract-lib",
  "module": "a_riverhog_linux_provenance_contract_lib",
  "name": "CONTRACT_BINDING",
  "unit": "export"
}
```

</details>
