# riverhog_provenance_macos_contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-macos-contracts:riverhog-provenance-macos-contracts:2493b8942f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-macos-contracts](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-904dc63cb3) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-74ec1d4419"></a>
| Field | Shape |
|---|---|
| <a id="s-1299c0dd57"></a>`candidate_id` | "python:riverhog-provenance-macos-contracts:riverhog_provenance_macos_contracts" |
| <a id="s-82a5e2713b"></a>`distribution` | "riverhog-provenance-macos-contracts" |
| <a id="s-3d809651a6"></a>`exports` | additional keys=`CONTRACT_BINDING`, `CONTRACT_ID`, `PLATFORM_FAMILY`, `load_schemas` |
| <a id="s-d666a91c65"></a>`module` | "riverhog_provenance_macos_contracts" |

## Governing policies

- <a id="pa-65a0194bc7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-macos-contracts:riverhog_provenance_macos_contracts](../../../evidence/sources.md#src-75fa891e7c) — `reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/__init__.py`

### Machine authority

- `/external_contract/python/21`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 29c80f7237137498b1a2d95ecb6d464268e0245338b3f98137a1899297a27786 -->

```json
{
  "candidate_id": "python:riverhog-provenance-macos-contracts:riverhog_provenance_macos_contracts",
  "distribution": "riverhog-provenance-macos-contracts",
  "exports": {
    "CONTRACT_BINDING": {
      "kind": "object",
      "type": "riverhog_provenance_contracts.ProvenanceContractBinding"
    },
    "CONTRACT_ID": {
      "kind": "constant",
      "value": "riverhog-provenance-macos-observation/v1"
    },
    "PLATFORM_FAMILY": {
      "kind": "constant",
      "value": "macos"
    },
    "load_schemas": {
      "kind": "function",
      "signature": "\"() -> 'dict[str, dict[str, Any]]'\""
    }
  },
  "module": "riverhog_provenance_macos_contracts"
}
```
