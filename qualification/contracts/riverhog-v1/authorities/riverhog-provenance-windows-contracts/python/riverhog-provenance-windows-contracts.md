# riverhog_provenance_windows_contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-windows-contracts:riverhog-provenance-windows-contracts:b8799d9700 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2e5e0a4832"></a>
| Field | Shape |
|---|---|
| <a id="s-14d8ec4a8b"></a>`candidate_id` | "python:riverhog-provenance-windows-contracts:riverhog_provenance_windows_contracts" |
| <a id="s-08ba1ea761"></a>`distribution` | "riverhog-provenance-windows-contracts" |
| <a id="s-908f6494d3"></a>`exports` | additional keys=`CONTRACT_BINDING`, `CONTRACT_ID`, `PLATFORM_FAMILY`, `load_schemas` |
| <a id="s-6e59ef568f"></a>`module` | "riverhog_provenance_windows_contracts" |

## Governing policies

- <a id="pa-fcaa39cdd0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-windows-contracts:riverhog_provenance_windows_contracts](../../../evidence/sources.md#src-0d5e61922a) — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/__init__.py`

### Machine authority

- `/external_contract/python/22`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9130cfcd39408faded315b5e916f686136d676dda5f35f52b1a2437cc87ceb2e -->

```json
{
  "candidate_id": "python:riverhog-provenance-windows-contracts:riverhog_provenance_windows_contracts",
  "distribution": "riverhog-provenance-windows-contracts",
  "exports": {
    "CONTRACT_BINDING": {
      "kind": "object",
      "type": "riverhog_provenance_contracts.ProvenanceContractBinding"
    },
    "CONTRACT_ID": {
      "kind": "constant",
      "value": "riverhog-provenance-windows-observation/v1"
    },
    "PLATFORM_FAMILY": {
      "kind": "constant",
      "value": "windows"
    },
    "load_schemas": {
      "kind": "function",
      "signature": "\"() -> 'dict[str, dict[str, Any]]'\""
    }
  },
  "module": "riverhog_provenance_windows_contracts"
}
```
