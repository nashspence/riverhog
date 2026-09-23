# a_riverhog_linux_provenance_contract_lib.CONTRACT_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-linux-provenance-contract-lib:a-riverhog-linux-provenance-contract-lib-contract-id:63edf50a19 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-linux-provenance-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e4ba6ce480"></a>
- <a id="s-555fa2ab9a"></a>`distribution`: `a-riverhog-linux-provenance-contract-lib`
- <a id="s-afe4819809"></a>`module`: `a_riverhog_linux_provenance_contract_lib`
- <a id="s-e0329d06bd"></a>`name`: `CONTRACT_ID`
- <a id="s-38d1ccbaa7"></a>`unit`: `export`

### Declared structure

- <a id="s-7fb4515734"></a>`kind`: `"constant"`
- <a id="s-f31afd21a6"></a>`value`: `"riverhog-provenance-linux-observation/v1"`

## Governing policies

- <a id="pa-2b08926904"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-linux-provenance-contract-lib:a_riverhog_linux_provenance_contract_lib](../../../evidence/sources/authorities.md#src-0abeb2023e) — [some-implementations/riverhog/provenance/contracts/linux/src/a\_riverhog\_linux\_provenance\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/provenance/contracts/linux/src/a_riverhog_linux_provenance_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_linux_provenance_contract_lib.CONTRACT_ID`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7335c4a15c51eb645e8887da0c000bfb515a05bf81bd363c9d9c8e9c783c8be8 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-provenance-linux-observation/v1"
  },
  "distribution": "a-riverhog-linux-provenance-contract-lib",
  "module": "a_riverhog_linux_provenance_contract_lib",
  "name": "CONTRACT_ID",
  "unit": "export"
}
```

</details>
