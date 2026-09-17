# riverhog_provenance_macos_contracts.CONTRACT_BINDING

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-macos-contracts:riverhog-provenance-macos-contracts-contract-binding:a3ad9fe20a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-macos-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8cea55c3f1"></a>
- <a id="s-1c3e4849e5"></a>`distribution`: `riverhog-provenance-macos-contracts`
- <a id="s-3b24792d1c"></a>`module`: `riverhog_provenance_macos_contracts`
- <a id="s-12cf986f2e"></a>`name`: `CONTRACT_BINDING`
- <a id="s-0e81c1215d"></a>`unit`: `export`

### Declared structure

- <a id="s-9ee3d45447"></a>`kind`: `"object"`
- <a id="s-eea49602e4"></a>`type`: `"riverhog_provenance_contracts.ProvenanceContractBinding"`

## Governing policies

- <a id="pa-c0617387ce"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance-macos-contracts:riverhog_provenance_macos_contracts](../../../evidence/sources/authorities.md#src-75fa891e7c) — [reference/riverhog/provenance/contracts/macos/src/riverhog\_provenance\_macos\_contracts/\_\_init\_\_.py](../../../../../../reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance_macos_contracts.CONTRACT_BINDING`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 40f3ebbd01e5d43920b513dff2c37df60c5dfb6a7b8515b79e34f66ac8c76489 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "riverhog_provenance_contracts.ProvenanceContractBinding"
  },
  "distribution": "riverhog-provenance-macos-contracts",
  "module": "riverhog_provenance_macos_contracts",
  "name": "CONTRACT_BINDING",
  "unit": "export"
}
```

</details>
