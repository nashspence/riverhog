# riverhog_provenance_contracts.ProvenanceJournalStateReference

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-contracts:riverhog-provenance-contracts-provenancej-0e2a1b02ae:f871a851d3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4b0964df26"></a>
| Field | Shape |
|---|---|
| <a id="s-bf572ea86b"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-0f5d21e7dc"></a>`distribution` | "riverhog-provenance-contracts" |
| <a id="s-78eb5c9244"></a>`module` | "riverhog_provenance_contracts" |
| <a id="s-ea79346969"></a>`name` | "ProvenanceJournalStateReference" |
| <a id="s-5bb7c3be33"></a>`unit` | "export" |

## Governing policies

- <a id="pa-ea38dba1d6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-contracts:riverhog_provenance_contracts](../../../evidence/sources.md#src-9b6289a988) — `packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance_contracts.ProvenanceJournalStateReference`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f3756f2b0aa6aa79415930b33506102c03247a378c477b908f037314d6be995 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "7f3d93e0df8f97bd47bb76100caba9e088809d1c50826896a8c184ecfb5acaf7",
    "signature": "'(*, journal_id: ProvenanceJournalId, current_state_id: ProvenanceStateId) -> None'"
  },
  "distribution": "riverhog-provenance-contracts",
  "module": "riverhog_provenance_contracts",
  "name": "ProvenanceJournalStateReference",
  "unit": "export"
}
```
