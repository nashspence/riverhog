# riverhog_provenance_windows_contracts.CONTRACT_BINDING

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-windows-contracts:riverhog-provenance-windows-contracts-con-52546ce278:feeb65d463 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b235145764"></a>
| Field | Shape |
|---|---|
| <a id="s-52773d6a04"></a>`contract` | type="riverhog_provenance_contracts.ProvenanceContractBinding"; additional keys=`kind` |
| <a id="s-cde1ba41b3"></a>`distribution` | "riverhog-provenance-windows-contracts" |
| <a id="s-7a0fbc7335"></a>`module` | "riverhog_provenance_windows_contracts" |
| <a id="s-afc893cdce"></a>`name` | "CONTRACT_BINDING" |
| <a id="s-377a978624"></a>`unit` | "export" |

## Governing policies

- <a id="pa-d2af1c4edd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-windows-contracts:riverhog_provenance_windows_contracts](../../../evidence/sources.md#src-0d5e61922a) — `reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance_windows_contracts.CONTRACT_BINDING`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b54fabb3c1aeb73ea77417c3b072d66823bf3ca0dca825f2e346d51b75c5353d -->

```json
{
  "contract": {
    "kind": "object",
    "type": "riverhog_provenance_contracts.ProvenanceContractBinding"
  },
  "distribution": "riverhog-provenance-windows-contracts",
  "module": "riverhog_provenance_windows_contracts",
  "name": "CONTRACT_BINDING",
  "unit": "export"
}
```
