# riverhog_provenance_macos_contracts.load_schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-macos-contracts:riverhog-provenance-macos-contracts-load-schemas:52b94b6ffa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-macos-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7d4f6022f1"></a>
- <a id="s-c11954a629"></a>`distribution`: `riverhog-provenance-macos-contracts`
- <a id="s-dab6eed4e8"></a>`module`: `riverhog_provenance_macos_contracts`
- <a id="s-87f398dadf"></a>`name`: `load_schemas`
- <a id="s-0e058febff"></a>`unit`: `export`

### Declared structure

- <a id="s-cc63df7f42"></a>`kind`: `"function"`
- <a id="s-20faf7689f"></a>`signature`: `"\"() -> 'dict[str, dict[str, Any]]'\""`

## Governing policies

- <a id="pa-da27960e75"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance-macos-contracts:riverhog_provenance_macos_contracts](../../../evidence/sources.md#src-75fa891e7c) — [reference/riverhog/provenance/contracts/macos/src/riverhog\_provenance\_macos\_contracts/\_\_init\_\_.py](../../../../../../reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance_macos_contracts.load_schemas`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e206a384b2ea06a5b92247f3c856ccba279ab81b387753ae009682ff3d5e93e8 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'dict[str, dict[str, Any]]'\""
  },
  "distribution": "riverhog-provenance-macos-contracts",
  "module": "riverhog_provenance_macos_contracts",
  "name": "load_schemas",
  "unit": "export"
}
```

</details>
