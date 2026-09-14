# riverhog_provenance_macos_contracts.CONTRACT_ID

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-macos-contracts:riverhog-provenance-macos-contracts-contract-id:9688b4e7e4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-macos-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-01f9424c24"></a>
- <a id="s-f4a97f3cd9"></a>`distribution`: `riverhog-provenance-macos-contracts`
- <a id="s-d37a68889d"></a>`module`: `riverhog_provenance_macos_contracts`
- <a id="s-1bf22b8f46"></a>`name`: `CONTRACT_ID`
- <a id="s-6aaabdfe0f"></a>`unit`: `export`

### Declared structure

- <a id="s-07daff3b89"></a>`kind`: `"constant"`
- <a id="s-1c4256f6d3"></a>`value`: `"riverhog-provenance-macos-observation/v1"`

## Governing policies

- <a id="pa-3aa85747e5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-macos-contracts:riverhog_provenance_macos_contracts](../../../evidence/sources.md#src-75fa891e7c) — `reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance_macos_contracts.CONTRACT_ID`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 738cf65a4eba137c9bdd2167940a3a2761d36b0868a504b24eb2b5daa5fd27ec -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-provenance-macos-observation/v1"
  },
  "distribution": "riverhog-provenance-macos-contracts",
  "module": "riverhog_provenance_macos_contracts",
  "name": "CONTRACT_ID",
  "unit": "export"
}
```
