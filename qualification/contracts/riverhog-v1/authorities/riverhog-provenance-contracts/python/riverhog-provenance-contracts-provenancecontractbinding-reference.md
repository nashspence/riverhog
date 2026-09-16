# riverhog_provenance_contracts.ProvenanceContractBinding.reference

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-contracts:riverhog-provenance-contracts-provenancec-858f586b3b:b3620cf1b5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-45a18acc45"></a>
- <a id="s-f9997832ff"></a>`distribution`: `riverhog-provenance-contracts`
- <a id="s-569025266e"></a>`module`: `riverhog_provenance_contracts`
- <a id="s-9b63b73b9b"></a>`name`: `reference`
- <a id="s-aabf5c8bda"></a>`owner`: `riverhog_provenance_contracts.ProvenanceContractBinding`
- <a id="s-3bdf79e389"></a>`unit`: `member`

### Declared structure

- <a id="s-a285bf6049"></a>`kind`: `"method"`
- <a id="s-cd6201cd6d"></a>`signature`: `"\"(self, provider: 'str') -> 'dict[str, str]'\""`

## Maintained corroboration

### Related interface records

- [ProvenanceContractBinding](riverhog-provenance-contracts-provenancecontractbinding.md)

## Governing policies

- <a id="pa-9e4b8383c4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-contracts:riverhog_provenance_contracts](../../../evidence/sources.md#src-9b6289a988) — `packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance_contracts.ProvenanceContractBinding.reference`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5032b25007a4b2477cb53e0af17354fc72bb6b3c59c241c34dd49eaaef52e83f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, provider: 'str') -> 'dict[str, str]'\""
  },
  "distribution": "riverhog-provenance-contracts",
  "module": "riverhog_provenance_contracts",
  "name": "reference",
  "owner": "riverhog_provenance_contracts.ProvenanceContractBinding",
  "unit": "member"
}
```

</details>
